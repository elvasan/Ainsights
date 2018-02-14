# pylint:disable=invalid-name,line-too-long
import os
import csv
import datetime
import boto3

s3_client = boto3.client('s3')
env = os.environ['ENV']
bucket_name = 'jornaya-' + env + '-us-east-1-aida-insights'
client_name = os.environ['CLIENT_NAME']
jornaya_account_id = os.environ['JORNAYA_ACCOUNT_ID']
config_data = []

# Response Industries
try:
    response_industries = os.environ['RESPONSE_INDUSTRIES'].split(',')
except KeyError:
    raise Exception('Must select at least 1 response industry.')

for industry in response_industries:
    config_data.append({'option': 'industry_result', 'config_abbrev': industry, 'value': industry})

# Event Lookback Period
event_lookback_auto_sales = os.environ['EVENT_LOOKBACK_AUTO_SALES'].strip()
event_lookback_education = os.environ['EVENT_LOOKBACK_EDUCATION'].strip()
event_lookback_financial_services = os.environ['EVENT_LOOKBACK_FINANCIAL_SERVICES'].strip()
event_lookback_home_services = os.environ['EVENT_LOOKBACK_HOME_SERVICES'].strip()
event_lookback_insurance = os.environ['EVENT_LOOKBACK_INSURANCE'].strip()
event_lookback_jobs = os.environ['EVENT_LOOKBACK_JOBS'].strip()
event_lookback_legal = os.environ['EVENT_LOOKBACK_LEGAL'].strip()
event_lookback_real_estate = os.environ['EVENT_LOOKBACK_REAL_ESTATE'].strip()
event_lookback_other = os.environ['EVENT_LOOKBACK_OTHER'].strip()

if event_lookback_auto_sales < 1 or not event_lookback_auto_sales.isdigit():
    raise Exception('event_lookback_auto_sales must be a positive integer')
if event_lookback_education < 1 or not event_lookback_education.isdigit():
    raise Exception('event_lookback_education must be a positive integer')
if event_lookback_financial_services < 1 or not event_lookback_financial_services.isdigit():
    raise Exception('event_lookback_financial_services must be a positive integer')
if event_lookback_home_services < 1 or not event_lookback_home_services.isdigit():
    raise Exception('event_lookback_home_services must be a positive integer')
if event_lookback_insurance < 1 or not event_lookback_insurance.isdigit():
    raise Exception('event_lookback_insurance must be a positive integer')
if event_lookback_jobs < 1 or not event_lookback_jobs.isdigit():
    raise Exception('event_lookback_jobs must be a positive integer')
if event_lookback_legal < 1 or not event_lookback_legal.isdigit():
    raise Exception('event_lookback_legal must be a positive integer')
if event_lookback_real_estate < 1 or not event_lookback_real_estate.isdigit():
    raise Exception('event_lookback_real_estate must be a positive integer')
if event_lookback_other < 1 or not event_lookback_other.isdigit():
    raise Exception('event_lookback_other must be a positive integer')

config_data.append({'option': 'event_lookback', 'config_abbrev': 'auto_sales', 'value': event_lookback_auto_sales})
config_data.append({'option': 'event_lookback', 'config_abbrev': 'education', 'value': event_lookback_education})
config_data.append({'option': 'event_lookback', 'config_abbrev': 'financial_services', 'value': event_lookback_financial_services})
config_data.append({'option': 'event_lookback', 'config_abbrev': 'home_services', 'value': event_lookback_home_services})
config_data.append({'option': 'event_lookback', 'config_abbrev': 'insurance', 'value': event_lookback_insurance})
config_data.append({'option': 'event_lookback', 'config_abbrev': 'jobs', 'value': event_lookback_jobs})
config_data.append({'option': 'event_lookback', 'config_abbrev': 'legal', 'value': event_lookback_legal})
config_data.append({'option': 'event_lookback', 'config_abbrev': 'real_estate', 'value': event_lookback_real_estate})
config_data.append({'option': 'event_lookback', 'config_abbrev': 'other', 'value': event_lookback_other})

# Frequency Threshold
frequency_threshold_auto_sales = os.environ['FREQUENCY_THRESHOLD_AUTO_SALES'].strip()
frequency_threshold_education = os.environ['FREQUENCY_THRESHOLD_EDUCATION'].strip()
frequency_threshold_financial_services = os.environ['FREQUENCY_THRESHOLD_FINANCIAL_SERVICES'].strip()
frequency_threshold_home_services = os.environ['FREQUENCY_THRESHOLD_HOME_SERVICES'].strip()
frequency_threshold_insurance = os.environ['FREQUENCY_THRESHOLD_INSURANCE'].strip()
frequency_threshold_jobs = os.environ['FREQUENCY_THRESHOLD_JOBS'].strip()
frequency_threshold_legal = os.environ['FREQUENCY_THRESHOLD_LEGAL'].strip()
frequency_threshold_real_estate = os.environ['FREQUENCY_THRESHOLD_REAL_ESTATE'].strip()
frequency_threshold_other = os.environ['FREQUENCY_THRESHOLD_OTHER'].strip()

if frequency_threshold_auto_sales < 1 or not frequency_threshold_auto_sales.isdigit():
    raise Exception('frequency_threshold_auto_sales must be a positive integer')
if frequency_threshold_education < 1 or not frequency_threshold_education.isdigit():
    raise Exception('frequency_threshold_education must be a positive integer')
if frequency_threshold_financial_services < 1 or not frequency_threshold_financial_services.isdigit():
    raise Exception('frequency_threshold_financial_services must be a positive integer')
if frequency_threshold_home_services < 1 or not frequency_threshold_home_services.isdigit():
    raise Exception('frequency_threshold_home_services must be a positive integer')
if frequency_threshold_insurance < 1 or not frequency_threshold_insurance.isdigit():
    raise Exception('frequency_threshold_insurance must be a positive integer')
if frequency_threshold_jobs < 1 or not frequency_threshold_jobs.isdigit():
    raise Exception('frequency_threshold_jobs must be a positive integer')
if frequency_threshold_legal < 1 or not frequency_threshold_legal.isdigit():
    raise Exception('frequency_threshold_legal must be a positive integer')
if frequency_threshold_real_estate < 1 or not frequency_threshold_real_estate.isdigit():
    raise Exception('frequency_threshold_real_estate must be a positive integer')
if frequency_threshold_other < 1 or not frequency_threshold_other.isdigit():
    raise Exception('frequency_threshold_other must be a positive integer')

config_data.append({'option': 'frequency_threshold', 'config_abbrev': 'auto_sales', 'value': frequency_threshold_auto_sales})
config_data.append({'option': 'frequency_threshold', 'config_abbrev': 'education', 'value': frequency_threshold_education})
config_data.append({'option': 'frequency_threshold', 'config_abbrev': 'financial_services', 'value': frequency_threshold_financial_services})
config_data.append({'option': 'frequency_threshold', 'config_abbrev': 'home_services', 'value': frequency_threshold_home_services})
config_data.append({'option': 'frequency_threshold', 'config_abbrev': 'insurance', 'value': frequency_threshold_insurance})
config_data.append({'option': 'frequency_threshold', 'config_abbrev': 'jobs', 'value': frequency_threshold_jobs})
config_data.append({'option': 'frequency_threshold', 'config_abbrev': 'legal', 'value': frequency_threshold_legal})
config_data.append({'option': 'frequency_threshold', 'config_abbrev': 'real_estate', 'value': frequency_threshold_real_estate})
config_data.append({'option': 'frequency_threshold', 'config_abbrev': 'other', 'value': frequency_threshold_other})

# as of date
try:
    as_of_date = os.environ['AS_OF_DATE'].strip()
    datetime.datetime.strptime(as_of_date, '%m/%d/%Y')
    config_data.append({'option': 'asof', 'config_abbrev': 'asof', 'value': as_of_date + ' 12:00'})
except KeyError:
    pass

# write csv file
csv_columns = ['option', 'config_abbrev', 'value']
with open('client_config.csv', 'wb') as client_config_file:
    writer = csv.DictWriter(client_config_file, csv_columns)
    writer.writeheader()
    for element in config_data:
        writer.writerow(element)

# upload to s3
s3_client.upload_file('client_config.csv',
                      bucket_name,
                      'app_data/%s/client_config.csv' % client_name,
                      ExtraArgs={
                          'GrantFullControl': 'id="'+jornaya_account_id+'"'
                      })
