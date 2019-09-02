# Makefile variables
PYTHON ?= python3

# Build variables
DOCKER_REGISTRY ?= hydrosphere
IMAGE_NAME ?= serving-writer
TAG ?= latest

# Server variables
DEBUG ?= 0
SERVER_PORT ?= 50051
SERVER_MAX_WORKERS ?= 1
STORAGE_TYPE ?= s3
STORAGE_BUCKET ?= workflow-orchestrator-test
STORAGE_ACCESS_KEY ?= \
	$(shell sed '2q;d' ~/.aws/credentials | cut -d ' ' -f 3)
STORAGE_SECRET_ACCESS_KEY ?= \
	$(shell sed '3q;d' ~/.aws/credentials | cut -d ' ' -f 3)
BUFFER_SIZE ?= 4194304

all: serve
serve: clean-parquet 
	DEBUG=$(DEBUG) SERVER_PORT=$(SERVER_PORT) SERVER_MAX_WORKERS=$(SERVER_MAX_WORKERS) \
	STORAGE_TYPE=$(STORAGE_TYPE) STORAGE_BUCKET=$(STORAGE_BUCKET) BUFFER_SIZE=$(BUFFER_SIZE) \
	STORAGE_ACCESS_KEY=$(STORAGE_ACCESS_KEY) STORAGE_SECRET_ACCESS_KEY=$(STORAGE_SECRET_ACCESS_KEY) \
	$(PYTHON) -m hydro_serving_writer.grpc_.server

build-container:
	@echo Started building new image
	docker build ${DOCKER_BUILD_OPTS} -t $(DOCKER_REGISTRY)/$(IMAGE_NAME):$(TAG) .
test-container-run:
	@echo Performing container run
	docker run -ti $(DOCKER_REGISTRY)/$(IMAGE_NAME):$(TAG) sh
push-container: 
	@echo Pushing image to the registry
	docker push $(DOCKER_REGISTRY)/$(IMAGE_NAME):$(TAG)

clean-parquet:
	rm -rf *.parquet