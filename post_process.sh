#!/usr/bin/env bash

FILE=$1

if [[ "$FILE" == *"models.rs" ]]; then
    sed -i 's/operand: models::ColumnExpression/operand: Box<models::ColumnExpression>/g' "$FILE"
    sed -i -E 's/([a-zA-Z0-9_]*)operand: intermediate_rep\.\1operand\.into_iter\(\)\.next\(\)/\1operand: intermediate_rep.\1operand.into_iter().next().map(Box::new)/g' "$FILE"
    sed -i 's/PartialOrd, serde/serde/g' "$FILE"
fi
