
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
	rm -fr lambda-dist/

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
	mkdir lambda-dist
	cp ./src/main.py ./dist
	cp -r ./samples/pyspark/config/ ./dist/config/
	cp ./emr-deploy/emr-config.json ./dist/config/emr-config.json
	cp ./emr-deploy/emr-instance-groups.json ./dist/config/emr-instance-groups.json
	cp ./emr-deploy/emr-instance-groups-boto.json ./dist/config/emr-instance-groups-boto.json
	cp ./emr-deploy/emr-ec2-attributes.json ./dist/config/emr-ec2-attributes.json
	zip -j ./lambda-dist/aida_insights_get_emr_cluster_state.zip ./lambda/aida_insights_get_emr_cluster_state/lambda_function.py
	zip -j ./lambda-dist/aida_insights_lambda_update_code.zip ./lambda/aida_insights_lambda_update_code/lambda_function.py
	zip -j ./lambda-dist/aida_insights_launch_emr_cluster.zip ./lambda/aida_insights_launch_emr_cluster/lambda_function.py
	zip -j ./lambda-dist/aida_insights_start_sfn.zip ./lambda/aida_insights_start_sfn/lambda_function.py
	zip -j ./lambda-dist/aida_insights_move_input_file.zip ./lambda/aida_insights_move_input_file/aida_insights_move_input_file.py
	cd ./src && zip -x main.py -r ../dist/jobs.zip .