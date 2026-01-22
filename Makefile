IMAGE_NAME ?= simple-dbms
IMAGE_TAG ?= latest
CONTAINER_NAME ?= simple_dbms
PORT ?= 8080

PWD := $(shell pwd)
DATA_DIR ?= $(PWD)/data

METASTORE_FILE := $(PWD)/metastore.json
TABLES_DIR := $(PWD)/tables
IMAGE := $(IMAGE_NAME):$(IMAGE_TAG)

.PHONY: build run stop logs shell clean init

build:
	docker build -t $(IMAGE) .

run: $(METASTORE_FILE)
	docker run -d --rm \
		--name $(CONTAINER_NAME) \
		-p $(PORT):8080 \
		-v $(METASTORE_FILE):/app/metastore.json \
		-v $(TABLES_DIR):/app/tables \
		-v $(DATA_DIR):/data \
		-e RUST_LOG=info \
		$(IMAGE_NAME)
stop:
	docker stop $(CONTAINER_NAME)

logs:
	docker logs -f $(CONTAINER_NAME)

shell:
	docker exec -it $(CONTAINER_NAME) /bin/sh

$(METASTORE_FILE):
	@echo "Creating empty metastore file..."
	@echo "{}" > $(METASTORE_FILE)

init:
	@mkdir -p $(TABLES_DIR)

clean:
	docker image rm $(IMAGE) || true

all: build
