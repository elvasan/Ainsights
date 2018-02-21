export lambdas ?= $(shell ls lambda)

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

	for lambda_name in $$lambdas; do	\
		zip -j ./lambda-dist/$$lambda_name.zip ./lambda/$$lambda_name/$$lambda_name.py;	\
	done

	cp ./src/main.py ./dist
	cp -r ./samples/pyspark/config/dev ./dist/config/dev
	cp -r ./samples/pyspark/config/qa ./dist/config/qa
	cp -r ./samples/pyspark/config/staging ./dist/config/staging
	cp -r ./samples/pyspark/config/prod ./dist/config/prod
	cp ./emr-deploy/emr-config.json ./dist/config/emr-config.json
	cp ./emr-deploy/emr-instance-groups-boto.json ./dist/config/emr-instance-groups-boto.json
	cp ./emr-deploy/emr-ec2-attributes.json ./dist/config/emr-ec2-attributes.json

	cd ./src && zip -x main.py -r ../dist/jobs.zip .

upload-dev: build
	aws s3 sync dist s3://jornaya-dev-us-east-1-aida-insights/pyspark/
	aws s3 sync lambda-dist s3://jornaya-dev-us-east-1-aida-insights/lambda

upload-qa: build
	aws s3 sync dist s3://jornaya-qa-us-east-1-aida-insights/pyspark/
	aws s3 sync lambda-dist s3://jornaya-qa-us-east-1-aida-insights/lambda

deploy-dev: upload-dev
	for lambda_name in $$lambdas; do	\
		aws lambda update-function-code --function-name $$lambda_name --s3-bucket jornaya-dev-us-east-1-aida-insights --s3-key lambda/$$lambda_name.zip --region us-east-1; \
	done

deploy-qa: upload-qa
	for lambda_name in $$lambdas; do	\
		aws lambda update-function-code --function-name $$lambda_name --s3-bucket jornaya-qa-us-east-1-aida-insights --s3-key lambda/$$lambda_name.zip --region us-east-1; \
	done
