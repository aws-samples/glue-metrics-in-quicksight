// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0.

import {Duration, NestedStack, RemovalPolicy, Stack, StackProps} from "aws-cdk-lib";
import {Construct} from "constructs";
import * as path from "path";
import {LAMBDA_DIR} from "../util/project";
import {
    Choice,
    Condition,
    DefinitionBody,
    JsonPath,
    LogLevel,
    Parallel,
    Pass,
    StateMachine
} from "aws-cdk-lib/aws-stepfunctions";
import {PythonFunction} from "@aws-cdk/aws-lambda-python-alpha";
import {IFunction, Runtime} from "aws-cdk-lib/aws-lambda";
import {ITable} from "aws-cdk-lib/aws-dynamodb";
import {PolicyStatement} from "aws-cdk-lib/aws-iam";
import {
    AthenaGetQueryResults,
    AthenaStartQueryExecution,
    CallAwsService,
    LambdaInvoke
} from "aws-cdk-lib/aws-stepfunctions-tasks";
import {CfnWorkGroup} from "aws-cdk-lib/aws-athena";
import {IBucket} from "aws-cdk-lib/aws-s3";
import {LogGroup, RetentionDays} from "aws-cdk-lib/aws-logs";

export interface StepFunctionStackProps extends StackProps {
    readonly ddbTable: ITable;
    readonly workgroup: CfnWorkGroup;
    readonly glueDatabaseName: string,
    readonly quicksightDatasetIdMetrics: string;
    readonly quicksightDatasetIdInfo: string;
    readonly resultsBucket: IBucket;
    readonly athenaFederatedCloudwatchFn: IFunction;
    readonly athenaFederatedDynamoDBFn: IFunction;
}
export class StepFunctions extends NestedStack {
    public readonly stateMachine: StateMachine;

    constructor(scope: Construct, id: string, props: StepFunctionStackProps) {
        super(scope, id, props);

        const {
            ddbTable,
            workgroup,
            glueDatabaseName,
            quicksightDatasetIdMetrics,
            quicksightDatasetIdInfo,
            resultsBucket,
            athenaFederatedCloudwatchFn,
            athenaFederatedDynamoDBFn
        } = props

        // ------------------------------------------
        // Glue Job Consumer Lambda
        // ------------------------------------------

        const glueJobConsumer = new PythonFunction(this, 'GlueJobConsumer', {
            entry: path.resolve(LAMBDA_DIR, "glueconsumer"),
            handler: 'lambda_handler',
            environment: {
                'DDB_TABLE': ddbTable.tableName,
                'LOG_LEVEL': 'DEBUG',
                'CUSTOM_COMPLETED_JOBS_PROCESS_TIME_RANGE': '1'
            },
            runtime: Runtime.PYTHON_3_11,
            memorySize: 512,
            timeout: Duration.minutes(10),
        })
        glueJobConsumer.addToRolePolicy(new PolicyStatement({
            actions: [
                'glue:ListJobs',
                'glue:GetJob',
                'glue:GetJobRuns',
            ],
            resources: [`arn:aws:glue:${Stack.of(this).region}:${Stack.of(this).account}:*`]
        }))
        glueJobConsumer.addToRolePolicy(new PolicyStatement({
            actions: [
                'glue:ListJobs',
            ],
            resources: ["*"]
        }))
        ddbTable.grantReadWriteData(glueJobConsumer)

        // ------------------------------------------
        // Step Functions State Machine
        // ------------------------------------------

        // ---- Start: Common ----

        const collectGlueMetadata = new LambdaInvoke(this, 'CollectGlueMetadata', {
            lambdaFunction: glueJobConsumer,
            outputPath: '$.Payload',
        })

        const parallel = new Parallel(this, 'Parallel')

        // ---- Branch 1: Cloudwatch Source ----

        const getMetricsTable = new AthenaStartQueryExecution(this, 'GetMetricsTable', {
            workGroup: workgroup.name,
            queryString: this.getMetricsTableQuery(glueDatabaseName, `s3://${resultsBucket.bucketName}/glue-cw-metrics/`),
            resultPath: `$.Payload`
        })

        const checkForNewMetrics = new AthenaStartQueryExecution(this, 'CheckForNewMetrics', {
            workGroup: workgroup.name,
            queryString: this.checkNewMetricsQuery(workgroup.name + '_cloudwatch'),
            resultPath: `$.Payload`
        })

        const getMetricsCount = new AthenaGetQueryResults(this, 'GetMetricsCount', {
            queryExecutionId: JsonPath.stringAt('$.Payload.QueryExecutionId'),
            resultPath: `$.Payload`,
        })
        getMetricsCount.addRetry({
            interval: Duration.seconds(10),
            maxAttempts: 10,
            maxDelay: Duration.seconds(15),
            backoffRate: 2
        })

        const insertNewMetricChoice = new Choice(this, 'Choice')

        const insertMetricsToS3Query = new AthenaStartQueryExecution(this, 'InsertMetricsToS3Query', {
            workGroup: workgroup.name,
            queryString: this.insertMetricsIntoS3Query(glueDatabaseName, workgroup.name + '_cloudwatch'),
            resultPath: `$.Payload`
        })

        const noNewMetrics = new Pass(this, 'NoNewMetrics')

        const quicksightIngestMetrics = new CallAwsService(this, 'QuicksightIngestMetrics', {
            iamResources: ["*"],
            service: 'quicksight',
            action: 'createIngestion',
            parameters: {
                AwsAccountId: Stack.of(this).account,
                DataSetId: quicksightDatasetIdMetrics,
                IngestionId: JsonPath.stringAt("$.info.cw_metrics_id"),
            },
        })

        const cloudwatchBranch = getMetricsTable
            .next(checkForNewMetrics)
            .next(getMetricsCount)
            .next(
                insertNewMetricChoice
                    .when(Condition.stringGreaterThan("$.Payload.ResultSet.Rows[1].Data[0].VarCharValue", "0"), insertMetricsToS3Query.next(quicksightIngestMetrics))
                    .otherwise(noNewMetrics)
            )

        // ---- Branch 2: DynamoDB Source ----

        const getRecordsTable = new AthenaStartQueryExecution(this, 'GetRecordsTable', {
            workGroup: workgroup.name,
            queryString: this.getRecordsTableQuery(glueDatabaseName, `s3://${resultsBucket.bucketName}/glue-job-info/`),
            resultPath: `$.Payload`
        })

        const insertRecordsChoice = new Choice(this, 'InsertRecordsChoice')

        const insertRecordsToS3Query = new AthenaStartQueryExecution(this, 'InsertRecordsToS3Query', {
            workGroup: workgroup.name,
            queryString: this.insertRecordsIntoS3Query(glueDatabaseName, workgroup.name + '_dynamodb', ddbTable.tableName),
            resultPath: `$.Payload`
        })

        const noNewRecords = new Pass(this, 'NoNewRecords')

        const quicksightIngestRecords = new CallAwsService(this, 'QuicksightIngestRecords', {
            iamResources: ["*"],
            service: 'quicksight',
            action: 'createIngestion',
            parameters: {
                AwsAccountId: Stack.of(this).account,
                DataSetId: quicksightDatasetIdInfo,
                IngestionId: JsonPath.stringAt("$.info.glue_info_id")
            }
        })

        const dynamoDBBranch = getRecordsTable
            .next(
                insertRecordsChoice
                    .when(Condition.stringGreaterThan("$.job_runs.total", "0"), insertRecordsToS3Query.next(quicksightIngestRecords))
                    .otherwise(noNewRecords)
            )

        // ---- Combined: Combining all branches ----

        const completeDefinition = collectGlueMetadata
            .next(
                parallel
                    .branch(cloudwatchBranch)
                    .branch(dynamoDBBranch)
            )

        const stateMachineLogGroup = new LogGroup(this, 'StateMachineLogGroup', {
            logGroupName: 'GlueBlogStateMachineLogGroup',
            removalPolicy: RemovalPolicy.DESTROY,
            retention: RetentionDays.ONE_DAY,
        })
        const stateMachine = new StateMachine(this, 'StateMachine', {
            definitionBody: DefinitionBody.fromChainable(completeDefinition),
            logs: {
                level: LogLevel.ALL,
                destination: stateMachineLogGroup
            }
        })

        stateMachine.addToRolePolicy(new PolicyStatement({
            actions: [
                'quicksight:CreateIngestion',
                'athena:GetDataCatalog',
            ],
            resources: ["*"] // SECURITY: CreateIngestion/GetDataCatalog cannot be scoped further
        }))
        stateMachine.addToRolePolicy(new PolicyStatement({
            actions: [
                'athena:StartQueryExecution',
                'athena:GetQueryResults',
                'athena:GetQueryExecution',
                'athena:StopQueryExecution',
            ],
            resources: [`arn:aws:athena:${Stack.of(this).region}:${Stack.of(this).account}:workgroup/${workgroup.name}`]
        }))
        stateMachine.addToRolePolicy(new PolicyStatement({
            actions: [
                'glue:CreateTable',
                'glue:GetTable',
                'glue:GetDatabase',
                'glue:BatchCreatePartition',
                'glue:GetPartition',
            ],
            resources: [
                `arn:aws:glue:${Stack.of(this).region}:${Stack.of(this).account}:catalog`,
                `arn:aws:glue:${Stack.of(this).region}:${Stack.of(this).account}:database/${glueDatabaseName}`,
                `arn:aws:glue:${Stack.of(this).region}:${Stack.of(this).account}:table/${glueDatabaseName}/glue_cw_metrics`,
                `arn:aws:glue:${Stack.of(this).region}:${Stack.of(this).account}:table/${glueDatabaseName}/glue_job_info`,
            ]
        }))
        stateMachine.addToRolePolicy(new PolicyStatement({
            actions: [
                "s3:GetBucketLocation",
                "s3:GetObject",
                "s3:ListBucket",
                "s3:ListBucketMultipartUploads",
                "s3:ListMultipartUploadParts",
                "s3:AbortMultipartUpload",
                "s3:PutObject",
            ],
            resources: [
                `arn:aws:s3:::${resultsBucket.bucketName}/*`,
                `arn:aws:s3:::${resultsBucket.bucketName}`
            ]
        }))

        glueJobConsumer.grantInvoke(stateMachine)
        athenaFederatedCloudwatchFn.grantInvoke(stateMachine)
        athenaFederatedDynamoDBFn.grantInvoke(stateMachine)

        this.stateMachine = stateMachine
    }

    private getMetricsTableQuery(glueDBName: string, glueMetricsLocationS3: string): string {
        return `CREATE EXTERNAL TABLE IF NOT EXISTS awsdatacatalog.${glueDBName}.glue_cw_metrics (
            job_name string,
            job_run_id string,
            cpu_executor_id string,
            heap_used_executor_id string,
            heap_usage_executor_id string,
            all_executors double,
            max_needed_executors double,
            agg_gb_read double,
            elapsed_minutes double,
            completed_stages double,
            completed_tasks double,
            failed_tasks double,
            stopped_tasks double,
            records_read double,
            diskspace_used_mb double,
            driver_heap_usage double,
            executor_heap_usage double,
            driver_heap_used double,
            executor_heap_used double,
            s3_read_gb double,
            s3_write_gb double,
            driver_cpu double,
            executor_cpu double,
            streaming_records double,
            streaming_batch_process_seconds double,
            \`timestamp\` timestamp
        )
        PARTITIONED BY (partition_hour string)
        ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        LOCATION '${glueMetricsLocationS3}'
        TBLPROPERTIES ( 'auto.purge' = 'false', 'parquet.compression' = 'SNAPPY', 'transactional' = 'false')`
    }

    private getRecordsTableQuery(glueDBName: string, glueJobInfoLocationS3: string): string {
        return `CREATE EXTERNAL TABLE IF NOT EXISTS awsdatacatalog.${glueDBName}.glue_job_info (
            job_run_id string,
            job_name string,
            is_autoscaled string,
            price_mult decimal(38,9),
            job_timeout decimal(38,9),
            exec_class string,
            job_end_time timestamp,
            job_runtime decimal(38,9),
            job_start_time timestamp,
            num_workers decimal(38,9),
            job_state string,
            dpu_seconds decimal(38,9),
            rate decimal(38,9),
            worker_type string,
            job_version string,
            run_attempt decimal(38,9),
            est_cost decimal(38,36),
            job_detail string
        )
        PARTITIONED BY ( partition_hour string )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        LOCATION '${glueJobInfoLocationS3}'
        TBLPROPERTIES ( 'auto.purge'='false', 'parquet.compression' = 'SNAPPY', 'transactional'='false')`
    }

    private checkNewMetricsQuery(cloudwatchDatasource: string): string {
        return `select count(*)
                FROM "${cloudwatchDatasource}"."default"."metric_samples"
                where namespace = 'Glue'
                  and (timestamp >= cast (to_unixtime(date_trunc('hour', current_timestamp) - interval '1' hour) as int) and timestamp < cast (to_unixtime(date_trunc('hour', current_timestamp) - interval '1' hour + interval '59' minute + interval '59' second) as int))`
    }

    private insertMetricsIntoS3Query(glueDBName: string, cloudwatchDatasource: string): string {
        return `INSERT INTO awsdatacatalog.${glueDBName}.glue_cw_metrics
                SELECT dimensions[3].dim_value as job_name,
                       dimensions[2].dim_value as job_run_id,
                       case
                           when metric_name LIKE 'glue.%.system.cpuSystemLoad' and metric_name != 'glue.driver.system.cpuSystemLoad' and
                       metric_name != 'glue.ALL.system.cpuSystemLoad' and statistic = 'Average' then (metric_name) end as cpu_executor_id, case when metric_name LIKE 'glue.%.jvm.heap.used' and metric_name != 'glue.driver.jvm.heap.used' and metric_name != 'glue.ALL.jvm.heap.used' and statistic = 'Average' then (metric_name) end as heap_used_executor_id, case when metric_name LIKE 'glue.%.jvm.heap.usage' and metric_name != 'glue.driver.jvm.heap.usage' and metric_name != 'glue.ALL.jvm.heap.usage' and statistic = 'Average' then (metric_name) end as heap_usage_executor_id, AVG (case when metric_name = 'glue.driver.ExecutorAllocationManager.executors.numberAllExecutors' and statistic = 'Average' then (value) end) as all_executors, MAX (case when metric_name = 'glue.driver.ExecutorAllocationManager.executors.numberMaxNeededExecutors' and statistic = 'Maximum' then (value) end) as max_needed_executors, SUM (case when metric_name = 'glue.driver.aggregate.bytesRead' and statistic = 'Sum' then (value /1024 /1024 /1024) end) as agg_gb_read, SUM (case when metric_name = 'glue.driver.aggregate.elapsedTime' and statistic = 'Sum' then (value / 60000) end) as elapsed_minutes, SUM (case when metric_name = 'glue.driver.aggregate.numCompletedStages' and statistic = 'Sum' then (value) end) as completed_stages, SUM (case when metric_name = 'glue.driver.aggregate.numCompletedTasks' and statistic = 'Sum' then (value) end) as completed_tasks, SUM (case when metric_name = 'glue.glue.driver.aggregate.numFailedTasks' and statistic = 'Sum' then (value) end) as failed_tasks, SUM (case when metric_name = 'glue.driver.aggregate.numKilledTasks' and statistic = 'Sum' then (value) end) as stopped_tasks, SUM (case when metric_name = 'glue.driver.aggregate.recordsRead' and statistic = 'Sum' then (value) end) as records_read, AVG (case when metric_name = 'glue.driver.BlockManager.disk.diskSpaceUsed_MB' and statistic = 'Average' then (value) end) as diskspace_used_mb, AVG (case when metric_name = 'glue.driver.jvm.heap.usage' and statistic = 'Average' then (value) end) as driver_heap_usage, AVG (case when metric_name LIKE 'glue.%.jvm.heap.usage' and metric_name != 'glue.driver.jvm.heap.usage' and metric_name != 'glue.ALL.jvm.heap.usage' and statistic = 'Average' then (value) end) as executor_heap_usage, AVG (case when metric_name = 'glue.driver.jvm.heap.used' and statistic = 'Average' then (value) end) as driver_heap_used, AVG (case when metric_name LIKE 'glue.%.jvm.heap.used' and metric_name != 'glue.driver.jvm.heap.used' and metric_name != 'glue.ALL.jvm.heap.used' and statistic = 'Average' then (value) end) as executor_heap_used, SUM (case when metric_name = 'glue.ALL.s3.filesystem.read_bytes' and statistic = 'Sum' then (value /1024 /1024 /1024) end) as s3_read_gb, SUM (case when metric_name = 'glue.ALL.s3.filesystem.write_bytes' and statistic = 'Sum' then (value /1024 /1024 /1024) end) as s3_write_gb, AVG (case when metric_name = 'glue.driver.system.cpuSystemLoad' and statistic = 'Average' then (value) end) as driver_cpu, AVG (case when metric_name LIKE 'glue.%.system.cpuSystemLoad' and metric_name != 'glue.driver.system.cpuSystemLoad' and metric_name != 'glue.ALL.system.cpuSystemLoad' and statistic = 'Average' then (value) end) as executor_cpu, SUM (case when metric_name = 'glue.driver.streaming.numRecords' and statistic = 'Sum' then (value) end) as streaming_records, AVG (case when metric_name = 'glue.driver.streaming.batchProcessingTimeInMs' and statistic = 'Average' then (value /1000) end) as streaming_batch_process_seconds, cast (date_format(from_unixtime(timestamp), '%Y-%m-%d %H:%i:00') as timestamp) AS timestamp, date_format(date_add('hour', -1, current_timestamp), '%Y-%m-%d %H') as partition_hour
                FROM "${cloudwatchDatasource}"."default"."metric_samples"
                where namespace = 'Glue'
                  and (timestamp >= cast (to_unixtime(date_trunc('hour'
                    , current_timestamp) - interval '1' hour) as int)
                  and timestamp
                    < cast (to_unixtime(date_trunc('hour'
                    , current_timestamp) - interval '1' hour + interval '59' minute + interval '59' second) as int))
                  and dimensions[2].dim_value != 'ALL'
                group by 1, 2, 3, 4, 5, date_format(from_unixtime(timestamp), '%Y-%m-%d %H:%i:00')`
    }

    private insertRecordsIntoS3Query(glueDBName: string, dynamoDBDatasource: string, dynamoDBTableName: string): string {
        return `INSERT INTO awsdatacatalog.${glueDBName}.glue_job_info
                select job_run_id,
                       job_name,
                       is_autoscaled,
                       price_mult,
                       job_timeout,
                       exec_class,
                       cast(job_end_time as timestamp)                                     as job_end_time,
                       job_runtime / 60                                                    as job_runtime,
                       cast(job_start_time as timestamp)                                   as job_start_time,
                       num_workers,
                       job_state,
                       job_type,
                       dpu_seconds,
                       rate,
                       worker_type,
                       job_version,
                       run_attempt,
                       case
                           when job_runtime = 0 then 0
                           when job_version > '2.0' then case
                                                             when job_runtime = 0 then 0
                                                             when is_autoscaled = 'true' and dpu_seconds > 0 and (job_runtime / 60) > 1
                                                                 then ((dpu_seconds / 60) / 60) * rate
                                                             when is_autoscaled = 'false' and (job_runtime / 60) > 1
                                                                 then ((job_runtime / 60) / 60) * num_workers * rate * price_mult
                                                             else (1 / 60) * num_workers * rate * price_mult end
                           else case
                                    when (job_runtime / 60) > 10 then ((job_runtime / 60) / 60) * num_workers * rate
                                    else (10 / 60) * num_workers * rate end end            as est_cost,
                       ('Job state: ' || job_state || ', ' || 'Worker type: ' || worker_type || ', ' ||
                        'Job is autoscaled?: ' || is_autoscaled || ', ' || 'Execution class: ' || exec_class || ', ' ||
                        'Job type: ' || job_type)                                          as job_detail,
                       date_format(date_add('hour', -1, current_timestamp), '%Y-%m-%d %H') as partition_hour
                from "${dynamoDBDatasource}"."default"."${dynamoDBTableName}"
                where (cast(job_end_time as timestamp) >= date_trunc('hour'
                                                              , current_timestamp) - interval '1' hour
                    and cast(job_end_time as timestamp)
                           < date_trunc('hour'
                                 , current_timestamp) - interval '1' hour + interval '59' minute + interval '59' second)
                   OR job_state = 'RUNNING'`
    }
}
