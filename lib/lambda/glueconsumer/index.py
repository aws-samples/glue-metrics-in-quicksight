# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import json
import logging
from uuid import uuid4
from os import environ
from decimal import Decimal
from time import time
from datetime import datetime, timedelta, timezone

# Get ENV VARS
region = environ.get("AWS_REGION")
ddb_table = environ.get("DDB_TABLE")
log_level = environ.get("LOG_LEVEL")

# Assertion
assert region is not None, "Please set REGION environment variable."
assert ddb_table is not None, "Please set DDB_TABLE environment variable."
assert log_level is not None, "Please set LOG_LEVEL environment variable (DEBUG, INFO, WARN, and ERROR)."

# Instantiate Boto3 Clients
glue = boto3.client("glue")
ddb = boto3.resource("dynamodb")
table = ddb.Table(ddb_table)

# Instantiate Logger
logger = logging.getLogger()
level = logging.getLevelName(log_level)
logger.setLevel(level)

match region:
    case 'us-east-1':
        region_rate = Decimal('0.44')
        region_rate_flex = Decimal('0.29')
    case 'us-east-2':
        region_rate = Decimal('0.44')
        region_rate_flex = Decimal('0.29')
    case 'us-west-1':
        region_rate = Decimal('0.44')
        region_rate_flex = Decimal('0.29')
    case 'us-west-2':
        region_rate = Decimal('0.44')
        region_rate_flex = Decimal('0.29')
    case 'af-south-1':
        region_rate = Decimal('0.524')
        region_rate_flex = Decimal('0.29')
    case 'ap-east-1':
        region_rate = Decimal('0.44')
        region_rate_flex = Decimal('0.29')
    case 'ap-south-2':
        region_rate = Decimal('0.44')
        region_rate_flex = Decimal('0.29')
    case 'ap-southeast-3':
        region_rate = Decimal('0.44')
        region_rate_flex = Decimal('0.29')
    case 'ap-southeast-4':
        region_rate = Decimal('0.44')
        region_rate_flex = Decimal('0.29')
    case 'ap-south-1':
        region_rate = Decimal('0.44')
        region_rate_flex = Decimal('0.29')
    case 'ap-northeast-3':
        region_rate = Decimal('0.44')
        region_rate_flex = Decimal('0.29')
    case 'ap-northeast-2':
        region_rate = Decimal('0.44')
        region_rate_flex = Decimal('0.29')
    case 'ap-southeast-1':
        region_rate = Decimal('0.44')
        region_rate_flex = Decimal('0.29')
    case 'ap-southeast-2':
        region_rate = Decimal('0.44')
        region_rate_flex = Decimal('0.29')
    case 'ap-northeast-1':
        region_rate = Decimal('0.44')
        region_rate_flex = Decimal('0.29')
    case 'ca-central-1':
        region_rate = Decimal('0.44')
        region_rate_flex = Decimal('0.29')
    case 'eu-central-1':
        region_rate = Decimal('0.44')
        region_rate_flex = Decimal('0.29')
    case 'eu-west-1':
        region_rate = Decimal('0.44')
        region_rate_flex = Decimal('0.29')
    case 'eu-west-2':
        region_rate = Decimal('0.44')
        region_rate_flex = Decimal('0.29')
    case 'eu-south-1':
        region_rate = Decimal('0.52')
        region_rate_flex = Decimal('0.29')
    case 'eu-west-3':
        region_rate = Decimal('0.44')
        region_rate_flex = Decimal('0.29')
    case 'eu-south-2':
        region_rate = Decimal('0.44')
        region_rate_flex = Decimal('0.29')
    case 'eu-north-1':
        region_rate = Decimal('0.44')
        region_rate_flex = Decimal('0.29')
    case 'eu-central-1':
        region_rate = Decimal('0.44')
        region_rate_flex = Decimal('0.29')
    case 'il-central-1':
        region_rate = Decimal('0.44')
        region_rate_flex = Decimal('0.29')
    case 'me-south-1':
        region_rate = Decimal('0.59')
        region_rate_flex = Decimal('0.29')
    case 'me-central-1':
        region_rate = Decimal('0.44')
        region_rate_flex = Decimal('0.29')
    case 'sa-east-1':
        region_rate = Decimal('0.69')
        region_rate_flex = Decimal('0.45')
    case _:
        logger.warning("Region not recognized, setting default rate of 0.44 and flex rate of 0.29")
        region_rate = Decimal('0.44')
        region_rate_flex = Decimal('0.29')

# Capture current time for comparison used later
# Environment variable CUSTOM_COMPLETED_JOBS_PROCESS_TIME_RANGE offers a way to
#   adjust the range of time specified for items to be processed.
#   Default is set to 1 hour but can be adjusted if you need to retroactively process missed data.
#   Please set back to "1" after you've reconciled any missed data.
current_time = datetime.now(tz=timezone.utc).isoformat()
streaming_placeholder = str(datetime.strptime(current_time, '%Y-%m-%dT%H:%M:%S.%f%z'))
ct_less_one_day = datetime.strptime(current_time, '%Y-%m-%dT%H:%M:%S.%f%z') - timedelta(days=1)
custom_completed_jobs_process_time_range = datetime.strptime(current_time, '%Y-%m-%dT%H:%M:%S.%f%z') - timedelta(hours=int(environ.get('CUSTOM_COMPLETED_JOBS_PROCESS_TIME_RANGE')))
logger.debug("Current time: " + str(current_time))
logger.debug("Current time less one day: " + str(ct_less_one_day))
logger.debug("Custom time range set to process after: " + str(custom_completed_jobs_process_time_range))

# Valid AWS Glue job states
valid_job_states = ['FAILED', 'SUCCEEDED', 'STOPPED', 'TIMEOUT']

# Entrypoint
def lambda_handler(event, context):
    # Get all jobs, paginate if applicable.
    glue_jobs = glue.list_jobs()
    jobs = glue_jobs['JobNames']
  

    # list_jobs does not support pagination, using NextToken
    while 'NextToken' in glue_jobs:
        glue_jobs = glue.list_jobs(NextToken=glue_jobs['NextToken'])
        jobs.extend(glue_jobs['JobNames'])

    logger.debug("Number of jobs: " + str(len(jobs)))
    print("Number of jobs: " + str(len(jobs)))

    # Iterate through job runs for each job and build a list of runs
    job_runs = []
    for job in jobs:
        runs = (
            glue.get_paginator('get_job_runs')
            .paginate(JobName=job)
            .build_full_result()
        )
        for run in runs['JobRuns']:
            try:
                if run['JobRunState'] == 'RUNNING' or run['CompletedOn'] > ct_less_one_day or run['CompletedOn'] > custom_completed_jobs_process_time_range:
                    job_runs.append(run)
                
            except Exception as e:
                logger.warning("Job Run " + run['Id'] + " did not meet validation check... moving on.")

    logger.debug("Individual job runs in the last day: " + str(len(job_runs)))

    # Filter out jobs that don't have the valid states (FAILED, STOPPED, SUCCEEDED)
    # Note: Update environment variable "CUSTOM_COMPLETED_JOBS_PROCESS_TIME_RANGE"
    #   if you need to extend the time in which this job processes changes
    #   For example, in the event that some scheduled run times are missed
    try:
        completed_jobs = []
        for run in job_runs:
            if run['JobRunState'] in valid_job_states:
                if run['CompletedOn'] > custom_completed_jobs_process_time_range:
                    completed_jobs.append(run)
        logger.debug("Jobs SUCCEEDED/FAILED/STOPPED in the last day: " + str(len(list(completed_jobs))))
    except Exception as e:
        logger.error("Unexpected error filtering jobs: " + e)
        raise e

    # Also grab list of streamingetl jobs in RUNNING status
    try:
        running_jobs = []
        for run in job_runs:
            if run['JobRunState'] == "RUNNING":
                running_jobs.append(run)
    except Exception as e:
        logger.error("Unexpected error filtering jobs: " + e)
        raise e

    logger.debug("Running jobs: " + str(len(list(running_jobs))))

    try:
        running_gluestreaming_jobs = []
        for run in list(running_jobs):
            job_type = glue.get_job(JobName=run['JobName'])
            if job_type['Job']['Command']['Name'] == "gluestreaming":
                running_gluestreaming_jobs.append(run)
    except Exception as e:
        logger.error("Unexpected error when analyzing streaming jobs: " + e)
        raise e

    # Combine jobs completed in the last hour with running streamingetl jobs
    if running_gluestreaming_jobs:
        logger.info("Glue Streaming jobs detected ({}), appending...".format(str(len(running_gluestreaming_jobs))))
        jobs_to_be_committed = running_gluestreaming_jobs + list(completed_jobs)
    else:
        logger.info("No Glue Streaming jobs detected, moving on...")
        jobs_to_be_committed = completed_jobs

    # Commit jobs to DynamoDB table
    ids = []
    items_committed = []
    for job in list(jobs_to_be_committed):
        response = commit_job(job)
        ids.append(job['Id'])
        items_committed.append(response)

    len_items_committed = str(len(items_committed))
    logger.info("Job runs captured: " + '\n'.join(ids))
    logger.info("Items committed: " + len_items_committed)

    return {
        "job_runs": {
            "total": len_items_committed
        },
        "request_id": context.aws_request_id,
        "info": {
            "description": "IDs generated for downstream step function components.",
            "glue_info_id": str(uuid4()),
            "cw_metrics_id": str(uuid4())
        }
    }


def commit_job(job_run: dict) -> dict:
    try:
        dump = json.dumps(job_run, default=str)
        formatted_dump = json.loads(dump, parse_float=lambda x: round(Decimal(x), 2))
    except Exception as error:
        logger.error("Unable to format job details.")
        raise error

    assert formatted_dump, "Job details are empty. Exiting"

    try:
        details = {
            "job_name": formatted_dump['JobName'],
            "job_run_id": formatted_dump['Id'],
            "num_workers": formatted_dump['NumberOfWorkers'],
            "run_attempt": formatted_dump['Attempt'],
            "job_state": formatted_dump['JobRunState'],
            "worker_type": formatted_dump['WorkerType'],
            "job_runtime": formatted_dump['ExecutionTime'],
            "job_start_time": formatted_dump['StartedOn'],
            "job_version": formatted_dump.get('GlueVersion', '0.9'),
            "exec_class": formatted_dump.get('ExecutionClass', 'STANDARD'),
            "glue_id": str(formatted_dump['Id'] + '_' + formatted_dump['StartedOn']),
            "job_type": glue.get_job(JobName=formatted_dump['JobName'])['Job']['Command']['Name'],
            "ttl": int(time()) + 604800  # epoch plus a week
        }
    except Exception as error:
        logger.error("Unable to marshal job details.")
        raise error

    # Catch CompletedOn if job isn't in RUNNING state
    if 'CompletedOn' in formatted_dump:
        details['job_end_time'] = formatted_dump['CompletedOn']
    else:
        details['job_end_time'] = streaming_placeholder

    # Catch timeout if job isn't in RUNNING state
    if 'Timeout' in formatted_dump:
        details['job_timeout'] = formatted_dump['Timeout']
    else:
        details['job_timeout'] = 0

    # Is job auto-scaled?
    if 'DPUSeconds' in formatted_dump:
        details['dpu_seconds'] = formatted_dump['DPUSeconds']
        details['is_autoscaled'] = 'true'
    else:
        details['dpu_seconds'] = 0
        details['is_autoscaled'] = 'false'

    # Rate at which the job class is charged
    match details['exec_class']:
        case 'STANDARD':
            rate = region_rate
        case 'FLEX':
            rate = region_rate_flex
        case _:
            logger.warning("Unrecognized or empty execution class. Setting default.")
            rate = Decimal('0.44')

    details['rate'] = rate

    # add worker type multiplier for downstream report
    # As more worker types are supported in the future, you'll need to come back and adjust this case statement.
    match formatted_dump['WorkerType']:
        case 'G.1X':
            details['price_mult'] = 1
        case 'G.2X':
            details['price_mult'] = 2
        case 'G.4X':
            details['price_mult'] = 4
        case 'G.8X':
            details['price_mult'] = 8
        case 'Z.2X':
            details['price_mult'] = 2
        case 'G.025X':
            details['price_mult'] = 0.25
        case _:
            details['price_mult'] = 1
            logger.warning("Worker type not recognized... setting default.")

    try:
        response = table.put_item(Item=details)
        return response
    except Exception as error:
        logger.error("ERROR: Unable to put item to DynamoDB table.", error)
        raise error