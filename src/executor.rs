use log::{error, info};
use std::{cmp::Ordering, collections::HashMap, fs::File, rc::Rc};

use csv::ReaderBuilder;

use crate::{metastore, planner, query, utils::convert_to_table_file_table};

#[derive(Clone)]
pub struct Executor {}

impl Executor {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn execute(
        &self,
        query_id: &String,
        plan: planner::PhysicalPlan,
        metastore: &metastore::SharedMetastore,
    ) {
        if let Err(e) = self
            .set_status(query_id, query::QueryStatus::Running, metastore)
            .await
        {
            error!("Failed to start query {}: {:?}", query_id, e);
            self.fail_query(
                query_id,
                "Query was deleted before execution".to_string(),
                metastore,
            )
            .await;
            return;
        }

        let result = match plan {
            planner::PhysicalPlan::SelectAll(select_all) => {
                self.select_all(query_id, &select_all, metastore).await
            }
            planner::PhysicalPlan::Select(select) => {
                self.select(query_id, &select, metastore).await
            }
            planner::PhysicalPlan::CopyFromCsv(copy) => {
                let res = self.copy_from_csv(query_id, &copy, metastore).await;
                if let Some(access_set) = metastore
                    .write()
                    .await
                    .table_accesses
                    .get_mut(&copy.table_id)
                {
                    access_set.remove(query_id);
                }
                res
            }
        };

        match result {
            Ok(query_result) => {
                self.complete_query(query_id, query_result, metastore).await;
            }
            Err(e) => {
                self.fail_query(query_id, e, metastore).await;
            }
        };
    }

    async fn select_all(
        &self,
        _: &String,
        select_all_plan: &planner::SelectAllPlan,
        _: &metastore::SharedMetastore,
    ) -> Result<Option<Vec<query::QueryResult>>, String> {
        Ok(Some(vec![query::QueryResult {
            table_id: select_all_plan.table_id.clone(),
        }]))
    }

    async fn select(
        &self,
        query_id: &String,
        select_plan: &planner::SelectPlan,
        metastore: &metastore::SharedMetastore,
    ) -> Result<Option<Vec<query::QueryResult>>, String> {
        let (result_columns, current_row_count) = self.execude_plan(select_plan, metastore).await?;

        let result_table_id = {
            let mut metastore_guard = metastore.write().await;
            let table_id = metastore_guard.create_query_result_table(
                query_id,
                result_columns,
                current_row_count,
            );
            metastore_guard
                .scheduled_for_deletion
                .insert(table_id.clone());
            if let Some(id) = &select_plan.table_id {
                if let Some(access_set) = metastore_guard.table_accesses.get_mut(id) {
                    access_set.remove(query_id);
                }
            }
            table_id
        };

        Ok(Some(vec![query::QueryResult {
            table_id: result_table_id,
        }]))
    }

    async fn execude_plan(
        &self,
        select_plan: &planner::SelectPlan,
        metastore: &metastore::SharedMetastore,
    ) -> Result<(Vec<lib::ColumnData>, usize), String> {
        let (mut working_columns, mut current_row_count) = if let Some(table_id) =
            &select_plan.table_id
        {
            let metastore_guard = metastore.read().await;
            let table = metastore_guard
                .get_table_internal(table_id)
                .ok_or(format!("Table {} not found during execution", table_id))?;

            let mut working_columns_innter: HashMap<String, Rc<lib::ColumnData>> = HashMap::new();
            for (col_name, &col_index) in &select_plan.column_indexes_map {
                let col_data = &table.columns[col_index].data;
                working_columns_innter.insert(col_name.clone(), Rc::new(col_data.clone()));
            }
            (working_columns_innter, table.get_num_rows() as usize)
        } else {
            (HashMap::new(), 0)
        };

        let mut expressions_results = vec![None; select_plan.expressions_map.len()];

        if let Some(filter_id) = select_plan.filter_expression {
            let filter_result = self.evaluate_expression(
                filter_id,
                &select_plan.expressions_map,
                &working_columns,
                current_row_count,
                &mut expressions_results,
            )?;

            let mask = match filter_result.as_ref() {
                lib::ColumnData::BOOL(vec) => vec.clone(),
                _ => return Err("Where clause did not evaluate correctly".to_string()),
            };

            drop(filter_result);

            expressions_results.fill(None);

            for (_, data_rc) in working_columns.iter_mut() {
                // Checking if strong references were properly dropped so no data is duplicated for
                // no reason
                debug_assert_eq!(Rc::strong_count(data_rc), 1, "Column cloned!");

                let data_mut = Rc::make_mut(data_rc);
                match data_mut {
                    lib::ColumnData::STR(raw) => self.apply_mask(raw, &mask),
                    lib::ColumnData::INT64(raw) => self.apply_mask(raw, &mask),
                    lib::ColumnData::BOOL(raw) => self.apply_mask(raw, &mask),
                }
            }
            current_row_count = mask.iter().filter(|&&b| b).count();
        }

        let evaluated_columns = select_plan
            .column_expressions
            .iter()
            .map(|&expr_id| {
                self.evaluate_expression(
                    expr_id,
                    &select_plan.expressions_map,
                    &working_columns,
                    current_row_count,
                    &mut expressions_results,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut row_indices = (0..current_row_count).collect::<Vec<_>>();

        if !select_plan.sorts.is_empty() {
            row_indices.sort_by(|&a, &b| {
                for sort in &select_plan.sorts {
                    let col = &evaluated_columns[sort.column_index];
                    let ordering = match col.as_ref() {
                        lib::ColumnData::INT64(vec) => vec[a].cmp(&vec[b]),
                        lib::ColumnData::STR(vec) => vec[a].cmp(&vec[b]),
                        lib::ColumnData::BOOL(vec) => vec[a].cmp(&vec[b]),
                    };

                    if ordering != Ordering::Equal {
                        return if sort.asscending {
                            ordering
                        } else {
                            ordering.reverse()
                        };
                    }
                }
                Ordering::Equal
            })
        }

        if let Some(limit) = select_plan.limit {
            if limit < row_indices.len() {
                row_indices.truncate(limit);
                current_row_count = limit;
            }
        }

        let result_columns = evaluated_columns
            .into_iter()
            .map(|col_rc| {
                let materialized_data = match col_rc.as_ref() {
                    lib::ColumnData::INT64(vec) => {
                        let new_vec = row_indices.iter().map(|&idx| vec[idx]).collect();
                        lib::ColumnData::INT64(new_vec)
                    }
                    lib::ColumnData::STR(vec) => {
                        let new_vec = row_indices.iter().map(|&idx| vec[idx].clone()).collect();
                        lib::ColumnData::STR(new_vec)
                    }
                    lib::ColumnData::BOOL(vec) => {
                        let new_vec = row_indices.iter().map(|&idx| vec[idx]).collect();
                        lib::ColumnData::BOOL(new_vec)
                    }
                };
                drop(col_rc);

                materialized_data
            })
            .collect::<Vec<_>>();

        Ok((result_columns, current_row_count))
    }

    fn evaluate_expression(
        &self,
        expr_id: usize,
        expressions_map: &Vec<planner::FlatExpression>,
        working_columns: &HashMap<String, Rc<lib::ColumnData>>,
        row_count: usize,
        expressions_results: &mut Vec<Option<Rc<lib::ColumnData>>>,
    ) -> Result<Rc<lib::ColumnData>, String> {
        if let Some(res) = &expressions_results[expr_id] {
            return Ok(res.clone());
        }

        let expr = &expressions_map[expr_id];

        match expr {
            planner::FlatExpression::Ref(col_name) => working_columns
                .get(col_name)
                .cloned()
                .ok_or("Column name does not exist in execution plan".to_string()),
            planner::FlatExpression::Literal(literal) => match literal {
                query::Literal::I64(val) => {
                    Ok(Rc::new(lib::ColumnData::INT64(vec![*val; row_count])))
                }
                query::Literal::String(val) => {
                    Ok(Rc::new(lib::ColumnData::STR(vec![val.clone(); row_count])))
                }
                query::Literal::Bool(val) => {
                    Ok(Rc::new(lib::ColumnData::BOOL(vec![*val; row_count])))
                }
            },
            planner::FlatExpression::Function(function_name, arg_ids) => {
                let args = arg_ids
                    .iter()
                    .map(|&id| {
                        self.evaluate_expression(
                            id,
                            expressions_map,
                            working_columns,
                            row_count,
                            expressions_results,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                self.evaluate_function(function_name, &args)
            }
            planner::FlatExpression::Binary(left_id, operator, right_id) => {
                let left_result = self.evaluate_expression(
                    *left_id,
                    expressions_map,
                    working_columns,
                    row_count,
                    expressions_results,
                )?;
                let rigt_result = self.evaluate_expression(
                    *right_id,
                    expressions_map,
                    working_columns,
                    row_count,
                    expressions_results,
                )?;
                self.evaluate_binary_expression(operator, left_result, rigt_result)
            }
            planner::FlatExpression::Unary(operator, id) => {
                let result = self.evaluate_expression(
                    *id,
                    expressions_map,
                    working_columns,
                    row_count,
                    expressions_results,
                )?;
                self.evaluate_unary_expression(operator, result)
            }
        }
    }

    fn evaluate_function(
        &self,
        function_name: &query::FunctionName,
        arguments_results: &Vec<Rc<lib::ColumnData>>,
    ) -> Result<Rc<lib::ColumnData>, String> {
        match (function_name, arguments_results.as_slice()) {
            (query::FunctionName::Concat, [left_rc, right_rc]) => {
                match (left_rc.as_ref(), right_rc.as_ref()) {
                    (lib::ColumnData::STR(l_vec), lib::ColumnData::STR(r_vec)) => {
                        let result_vec = l_vec
                            .iter()
                            .zip(r_vec.iter())
                            .map(|(l, r)| l.clone() + r)
                            .collect::<Vec<_>>();
                        Ok(Rc::new(lib::ColumnData::STR(result_vec)))
                    }
                    _ => Err("Concat requires (String, String)".to_string()),
                }
            }
            (query::FunctionName::Strlen, [arg_rc]) => match arg_rc.as_ref() {
                lib::ColumnData::STR(vec) => {
                    let result_vec = vec.iter().map(|v| v.len() as i64).collect();
                    Ok(Rc::new(lib::ColumnData::INT64(result_vec)))
                }
                _ => Err("Strln requires (String)".to_string()),
            },
            (query::FunctionName::Upper, [arg_rc]) => match arg_rc.as_ref() {
                lib::ColumnData::STR(vec) => {
                    let result_vec = vec.iter().map(|v| v.to_uppercase()).collect();
                    Ok(Rc::new(lib::ColumnData::STR(result_vec)))
                }
                _ => Err("Upper requires (String)".to_string()),
            },
            (query::FunctionName::Lower, [arg_rc]) => match arg_rc.as_ref() {
                lib::ColumnData::STR(vec) => {
                    let result_vec = vec.iter().map(|v| v.to_lowercase()).collect();
                    Ok(Rc::new(lib::ColumnData::STR(result_vec)))
                }

                _ => Err("Lower requires (String)".to_string()),
            },
            _ => Err("Wrong number of arguments".to_string()),
        }
    }

    fn evaluate_binary_expression(
        &self,
        operator: &query::BinOperator,
        left_res: Rc<lib::ColumnData>,
        right_res: Rc<lib::ColumnData>,
    ) -> Result<Rc<lib::ColumnData>, String> {
        match (left_res.as_ref(), right_res.as_ref()) {
            (lib::ColumnData::INT64(l_vec), lib::ColumnData::INT64(r_vec)) => match operator {
                query::BinOperator::Add
                | query::BinOperator::Subtract
                | query::BinOperator::Multiply
                | query::BinOperator::Divide => {
                    let result_vec = l_vec
                        .iter()
                        .zip(r_vec.iter())
                        .map(|(l, r)| match operator {
                            query::BinOperator::Add => Ok(*l + *r),
                            query::BinOperator::Subtract => Ok(*l - *r),
                            query::BinOperator::Multiply => Ok(*l * *r),
                            query::BinOperator::Divide => {
                                if *r == 0 {
                                    Err("Division by 0")
                                } else {
                                    Ok(*l / *r)
                                }
                            }
                            _ => unreachable!(),
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(Rc::new(lib::ColumnData::INT64(result_vec)))
                }
                query::BinOperator::Equal
                | query::BinOperator::NotEqual
                | query::BinOperator::LessThan
                | query::BinOperator::LessEqual
                | query::BinOperator::GreaterThan
                | query::BinOperator::GreaterEqual => {
                    let result_vec = l_vec
                        .iter()
                        .zip(r_vec.iter())
                        .map(|(l, r)| match operator {
                            query::BinOperator::Equal => l == r,
                            query::BinOperator::NotEqual => l != r,
                            query::BinOperator::LessThan => l < r,
                            query::BinOperator::LessEqual => l <= r,
                            query::BinOperator::GreaterThan => l > r,
                            query::BinOperator::GreaterEqual => l >= r,
                            _ => unreachable!(),
                        })
                        .collect::<Vec<_>>();
                    Ok(Rc::new(lib::ColumnData::BOOL(result_vec)))
                }

                _ => Err("Forbidden operation on integers".to_string()),
            },
            (lib::ColumnData::STR(l_vec), lib::ColumnData::STR(r_vec)) => {
                let result_vec = l_vec
                    .iter()
                    .zip(r_vec.iter())
                    .map(|(l, r)| match operator {
                        query::BinOperator::Equal => Ok(l == r),
                        query::BinOperator::NotEqual => Ok(l != r),
                        query::BinOperator::LessThan => Ok(l < r),
                        query::BinOperator::LessEqual => Ok(l <= r),
                        query::BinOperator::GreaterThan => Ok(l > r),
                        query::BinOperator::GreaterEqual => Ok(l >= r),
                        _ => Err("Forbidden operation on strings"),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Rc::new(lib::ColumnData::BOOL(result_vec)))
            }
            (lib::ColumnData::BOOL(l_vec), lib::ColumnData::BOOL(r_vec)) => {
                let result_vec = l_vec
                    .iter()
                    .zip(r_vec.iter())
                    .map(|(l, r)| match operator {
                        query::BinOperator::And => Ok(*l && *r),
                        query::BinOperator::Or => Ok(*l || *r),
                        query::BinOperator::Equal => Ok(l == r),
                        query::BinOperator::NotEqual => Ok(l != r),
                        query::BinOperator::LessThan => Ok(l < r),
                        query::BinOperator::LessEqual => Ok(l <= r),
                        query::BinOperator::GreaterThan => Ok(l > r),
                        query::BinOperator::GreaterEqual => Ok(l >= r),
                        _ => Err("Forbidden operation on booleans"),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Rc::new(lib::ColumnData::BOOL(result_vec)))
            }
            _ => Err("Missmatched operation types".to_string()),
        }
    }

    fn evaluate_unary_expression(
        &self,
        operator: &query::Operator,
        res: Rc<lib::ColumnData>,
    ) -> Result<Rc<lib::ColumnData>, String> {
        match (operator, res.as_ref()) {
            (query::Operator::Minus, lib::ColumnData::INT64(vec)) => {
                let result_vec = vec.iter().map(|v| -v).collect();
                Ok(Rc::new(lib::ColumnData::INT64(result_vec)))
            }
            (query::Operator::Not, lib::ColumnData::BOOL(vec)) => {
                let result_vec = vec.iter().map(|v| !v).collect();
                Ok(Rc::new(lib::ColumnData::BOOL(result_vec)))
            }
            _ => Err("Mismatched operation type".to_string()),
        }
    }

    fn apply_mask<T>(&self, data: &mut Vec<T>, mask: &[bool]) {
        let mut keep_idx = 0;
        for read_idx in 0..data.len() {
            if mask[read_idx] {
                if read_idx != keep_idx {
                    data.swap(read_idx, keep_idx);
                }
                keep_idx += 1;
            }
        }
        data.truncate(keep_idx);
    }

    async fn copy_from_csv(
        &self,
        query_id: &String,
        copy_plan: &planner::CopyFromCsvPlan,
        metastore: &metastore::SharedMetastore,
    ) -> Result<Option<Vec<query::QueryResult>>, String> {
        let file = File::open(&copy_plan.file_path)
            .map_err(|e| format!("Failed to open file '{}': {}", copy_plan.file_path, e))?;
        let mut rdr = ReaderBuilder::new()
            .has_headers(copy_plan.has_headers)
            .from_reader(file);
        let records = rdr
            .records()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("CSV Parse Error: {}", e))?
            .into_iter()
            .map(|r| r.iter().map(|s| s.to_string()).collect::<Vec<_>>())
            .collect::<Vec<_>>();

        let (mut shadow_columns, original_column_names) = {
            let metastore_guard = metastore.read().await;
            let table = metastore_guard
                .get_table_internal(&copy_plan.table_id)
                .ok_or_else(|| {
                    format!("Table {} not found during execution", copy_plan.table_id)
                })?;

            (
                table
                    .iter_columns()
                    .map(|column| match column.data {
                        lib::ColumnData::STR(_) => (
                            column.name.clone(),
                            lib::ColumnData::STR(Vec::with_capacity(records.len())),
                        ),
                        lib::ColumnData::INT64(_) => (
                            column.name.clone(),
                            lib::ColumnData::INT64(Vec::with_capacity(records.len())),
                        ),
                        lib::ColumnData::BOOL(_) => (
                            column.name.clone(),
                            lib::ColumnData::BOOL(Vec::with_capacity(records.len())),
                        ),
                    })
                    .collect::<HashMap<_, _>>(),
                table
                    .iter_columns()
                    .map(|column| column.name.clone())
                    .collect(),
            )
        };

        let csv_width = records[0].len();
        let num_rows = records.len() as u64;

        let csv_to_table_map: Vec<String> = match &copy_plan.mapping {
            Some(map_names) => {
                if map_names.len() != shadow_columns.len() {
                    return Err(format!(
                        "Invalid Mapping: You provided {} columns, but target table has {}. Mapping must describe every column in the target table.",
                        map_names.len(),
                        shadow_columns.len()
                    ));
                }
                if csv_width < map_names.len() {
                    return Err(format!(
                        "CSV too narrow: Mapping requires {} columns, but CSV only has {}.",
                        map_names.len(),
                        csv_width
                    ));
                }

                for name in map_names {
                    if !shadow_columns.contains_key(name) {
                        return Err(format!(
                            "Mapping references column '{}', which does not exist in table",
                            name
                        ));
                    }
                }
                map_names.clone()
            }
            None => {
                if csv_width != shadow_columns.len() {
                    return Err(format!(
                        "Mismatch: Table has {} columns, but CSV has {}. Without mapping, counts must match exactly.",
                        shadow_columns.len(),
                        csv_width
                    ));
                }

                original_column_names
            }
        };

        for (row_idx, record) in records.iter().enumerate() {
            if record.len() != csv_width {
                return Err(format!("Row {} length mismatch", row_idx + 1));
            }

            for (i, col_name) in csv_to_table_map.iter().enumerate() {
                let raw_val = &record[i];

                // We use unwrap() safely because we validated keys exist above
                let column_data = shadow_columns.get_mut(col_name).unwrap();

                match column_data {
                    lib::ColumnData::INT64(vec) => {
                        let val = raw_val.trim().parse::<i64>().map_err(|_| {
                            format!(
                                "Type Error at Row {}, Column '{}': Expected INT64, got '{}'",
                                row_idx + 1,
                                col_name,
                                raw_val
                            )
                        })?;
                        vec.push(val);
                    }
                    lib::ColumnData::STR(vec) => {
                        vec.push(raw_val.clone());
                    }
                    lib::ColumnData::BOOL(vec) => {
                        let val = raw_val.trim().parse::<bool>().map_err(|_| {
                            format!(
                                "Type Error at Row {}, Column '{}': Expected INT64, got '{}'",
                                row_idx + 1,
                                col_name,
                                raw_val
                            )
                        })?;
                        vec.push(val);
                    }
                }
            }
        }

        {
            let mut metastore_guard = metastore.write().await;
            let active_readers: Vec<String> =
                if let Some(readers) = metastore_guard.table_accesses.get(&copy_plan.table_id) {
                    readers
                        .iter()
                        .filter(|&id| *id != *query_id)
                        .cloned()
                        .collect()
                } else {
                    Vec::new()
                };

            if !active_readers.is_empty() {
                info!(
                    "COPY: Table {} has {} active readers. Creating snapshot.",
                    copy_plan.table_id,
                    active_readers.len()
                );

                let current_table = metastore_guard
                    .get_table_internal(&copy_plan.table_id)
                    .ok_or(format!("Table {} not found", copy_plan.table_id))?;

                let snapshot_id = uuid::Uuid::new_v4().to_string();
                let snapshot_metadata = metastore::TableMetaData {
                    name: copy_plan.table_name.clone(),
                    table: current_table.clone(),
                    table_file: convert_to_table_file_table(&snapshot_id),
                };

                metastore_guard
                    .tables
                    .insert(snapshot_id.clone(), snapshot_metadata);

                for reader_query_id in active_readers {
                    if let Some(query) = metastore_guard.queries.get_mut(&reader_query_id) {
                        if let Some(results) = &mut query.result {
                            for res in results {
                                if res.table_id == copy_plan.table_id {
                                    res.table_id = snapshot_id.clone();
                                }
                            }
                        }

                        match &mut query.definition {
                            query::QueryDefinition::SelectAll(select_all) => {
                                if select_all.table_id == copy_plan.table_id {
                                    select_all.table_id = snapshot_id.clone();
                                }
                            }
                            query::QueryDefinition::Select(select) => {
                                if let Some(current_table_id) = &select.table_id {
                                    if *current_table_id == copy_plan.table_id {
                                        select.table_id = Some(snapshot_id.clone());
                                    }
                                }
                            }
                            query::QueryDefinition::Copy(copy) => {
                                if copy.table_id == copy_plan.table_id {
                                    copy.table_id = snapshot_id.clone();
                                }
                            }
                        }
                    }

                    metastore_guard
                        .table_accesses
                        .entry(snapshot_id.clone())
                        .or_default()
                        .insert(reader_query_id);
                }

                metastore_guard.table_accesses.remove(&copy_plan.table_id);
                metastore_guard
                    .scheduled_for_deletion
                    .insert(snapshot_id.clone());
            }
        }

        {
            let mut metastore_guard = metastore.write().await;
            let table = metastore_guard
                .get_table_internal_mut(&copy_plan.table_id)
                .ok_or_else(|| format!("Table {} deleted during copy", copy_plan.table_id))?;

            for col in &mut table.columns {
                let mut new_data = shadow_columns.remove(&col.name).unwrap_or(match col.data {
                    lib::ColumnData::INT64(_) => {
                        lib::ColumnData::INT64(vec![0i64; num_rows as usize])
                    }
                    lib::ColumnData::STR(_) => {
                        lib::ColumnData::STR(vec!["".to_string(); num_rows as usize])
                    }
                    lib::ColumnData::BOOL(_) => {
                        lib::ColumnData::BOOL(vec![false; num_rows as usize])
                    }
                });

                match (&mut col.data, &mut new_data) {
                    (lib::ColumnData::INT64(existing_vec), lib::ColumnData::INT64(new_vec)) => {
                        existing_vec.append(new_vec);
                    }

                    (lib::ColumnData::STR(existing_vec), lib::ColumnData::STR(new_vec)) => {
                        existing_vec.append(new_vec);
                    }

                    (lib::ColumnData::BOOL(existing_vec), lib::ColumnData::BOOL(new_vec)) => {
                        existing_vec.append(new_vec);
                    }
                    _ => return Err("Columns types mismatched".to_string()),
                }
            }

            table.num_rows += num_rows;
        }

        Ok(None)
    }

    async fn set_status(
        &self,
        query_id: &String,
        status: query::QueryStatus,
        metastore: &metastore::SharedMetastore,
    ) -> Result<(), ()> {
        let mut metastore_guard = metastore.write().await;
        if let Some(q) = metastore_guard.get_query_internal_mut(query_id) {
            q.status = status;
            Ok(())
        } else {
            Err(())
        }
    }

    async fn complete_query(
        &self,
        query_id: &String,
        result: Option<Vec<query::QueryResult>>,
        metastore: &metastore::SharedMetastore,
    ) {
        let mut metastore_guard = metastore.write().await;
        if let Some(q) = metastore_guard.get_query_internal_mut(query_id) {
            q.status = query::QueryStatus::Completed;
            q.result = result;
            info!("Query {} completed successfully", query_id);
        }
    }

    async fn fail_query(
        &self,
        query_id: &String,
        error_msg: String,
        metastore: &metastore::SharedMetastore,
    ) {
        let mut metastore_guard = metastore.write().await;
        if let Some(q) = metastore_guard.get_query_internal_mut(query_id) {
            q.status = query::QueryStatus::Failed;
            q.errors = Some(vec![query::QueryError {
                message: error_msg.clone(),
                context: None,
            }]);
            let maybe_table_id = match &q.definition {
                query::QueryDefinition::SelectAll(select_all) => Some(select_all.table_id.clone()),
                query::QueryDefinition::Select(select) => select.table_id.clone(),
                query::QueryDefinition::Copy(copy) => Some(copy.table_id.clone()),
            };
            if let Some(id) = maybe_table_id {
                if let Some(access_set) = metastore_guard.table_accesses.get_mut(&id) {
                    access_set.remove(query_id);
                }
            }
            error!("Query {} failed: {}", query_id, error_msg);
        }
    }
}
