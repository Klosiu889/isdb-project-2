IMAGE_NAME ?= proj2
IMAGE_TAG ?= latest
PORT := 8080
DATA_DIR ?= ./data

IMAGE := $(IMAGE_NAME):$(IMAGE_TAG)

.PHONY: build run clean

build:
	docker build -t $(IMAGE) .

run:
	docker run -p $(PORT):$(PORT) -v $(DATA_DIR):/data $(IMAGE)

clean:
	docker image rm $(IMAGE) || true
