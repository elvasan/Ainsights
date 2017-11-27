
help:
	@echo "clean - remove all build, test, coverage and Python artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "lint - check style"
	@echo "test - run tests quickly with the default Python"
	@echo "build - package"

all: default

default: clean test lint build

clean: clean-build clean-pyc

clean-build:
	rm -fr dist/

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

lint:
	. prospector

test:
	python -m pytest ./tests/*

build: clean
	mkdir -p ./dist/config
	cp ./src/main.py ./dist
	cp ./samples/pyspark/config/application_defaults.csv ./dist/config/application_defaults.csv
	cp ./emr-deploy/emr-config.json ./dist/config/emr-config.json
	cp ./emr-deploy/emr-instance-groups.json ./dist/config/emr-instance-groups.json
	cp ./emr-deploy/emr-ec2-attributes.json ./dist/config/emr-ec2-attributes.json
	cd ./src && zip -x main.py -r ../dist/jobs.zip .