IMAGE_NAME ?= proj2
IMAGE_TAG ?= latest
CONTAINER_NAME ?= ISDB
PORT ?= 8080
DATA_DIR ?= ./data

IMAGE := $(IMAGE_NAME):$(IMAGE_TAG)

.PHONY: build run stop logs shell clean

build:
	docker build -t $(IMAGE) .

run:
	docker run -d --rm \
		--name $(CONTAINER_NAME) \
		-p $(PORT):8080 \
		-v ./metastore.json:/app/metastore.json \
		-v ./tables:/app/tables \
		-v $(DATA_DIR):/data \
		-e RUST_LOG=info \
		$(IMAGE_NAME)
stop:
	docker stop $(CONTAINER_NAME)

logs:
	docker logs -f $(CONTAINER_NAME)

shell:
	docker exec -it $(CONTAINER_NAME) /bin/sh

clean:
	docker image rm $(IMAGE) || true

all: build run
