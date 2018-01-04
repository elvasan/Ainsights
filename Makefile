
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
	cp -r ./samples/pyspark/config/dev ./dist/config/dev
	cp -r ./samples/pyspark/config/qa ./dist/config/qa
	cp -r ./samples/pyspark/config/staging ./dist/config/staging
	cp -r ./samples/pyspark/config/prod ./dist/config/prod
	cp ./emr-deploy/emr-config.json ./dist/config/emr-config.json
	cp ./emr-deploy/emr-instance-groups-boto.json ./dist/config/emr-instance-groups-boto.json
	cp ./emr-deploy/emr-ec2-attributes.json ./dist/config/emr-ec2-attributes.json
	zip -j ./lambda-dist/aida_insights_create_cloudwatch_event.zip ./lambda/aida_insights_create_cloudwatch_event/aida_insights_create_cloudwatch_event.py
	zip -j ./lambda-dist/aida_insights_get_emr_cluster_state.zip ./lambda/aida_insights_get_emr_cluster_state/aida_insights_get_emr_cluster_state.py
	zip -j ./lambda-dist/aida_insights_lambda_update_code.zip ./lambda/aida_insights_lambda_update_code/aida_insights_lambda_update_code.py
	zip -j ./lambda-dist/aida_insights_launch_emr_cluster.zip ./lambda/aida_insights_launch_emr_cluster/aida_insights_launch_emr_cluster.py
	zip -j ./lambda-dist/aida_insights_move_input_file.zip ./lambda/aida_insights_move_input_file/aida_insights_move_input_file.py
	zip -j ./lambda-dist/aida_insights_move_output_files.zip ./lambda/aida_insights_move_output_files/aida_insights_move_output_files.py
	zip -j ./lambda-dist/aida_insights_remove_cloudwatch_event.zip ./lambda/aida_insights_remove_cloudwatch_event/aida_insights_remove_cloudwatch_event.py
	zip -j ./lambda-dist/aida_insights_start_sfn_launch_cluster.zip ./lambda/aida_insights_start_sfn_launch_cluster/aida_insights_start_sfn_launch_cluster.py
	zip -j ./lambda-dist/aida_insights_start_sfn_package_output.zip ./lambda/aida_insights_start_sfn_package_output/aida_insights_start_sfn_package_output.py
	cd ./src && zip -x main.py -r ../dist/jobs.zip .