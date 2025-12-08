IMAGE_NAME ?= proj2
IMAGE_TAG ?= latest
NAME := ISDB
PORT := 8080
DATA_DIR ?= ./data

IMAGE := $(IMAGE_NAME):$(IMAGE_TAG)

.PHONY: build up down clean

build:
	docker-compose build

up:
	docker-compose up -d

down:
	docker-compose down

clean:
	docker-compose down --rmi local -v
