// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0.

import {NestedStack, Stack, StackProps} from 'aws-cdk-lib';
import {Construct} from 'constructs';
import {CfnDatabase, CfnTable} from "aws-cdk-lib/aws-glue";
import {ManagedPolicy, Role, ServicePrincipal} from "aws-cdk-lib/aws-iam";
import {IBucket} from "aws-cdk-lib/aws-s3";

export interface GlueTableProps extends StackProps {
    glueDatabaseName: string,
    resultBucket: IBucket,
}

export class GlueTables extends NestedStack {
    public readonly GlueDatabase: CfnDatabase

    constructor(scope: Construct, id: string, props: GlueTableProps) {
        super(scope, id, props);

        const { glueDatabaseName, resultBucket } = props

        const role = new Role(this, "GlueBlogRole", {
            assumedBy: new ServicePrincipal("glue.amazonaws.com"),
        })
        role.addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSGlueServiceRole"))

        this.GlueDatabase = new CfnDatabase(this, 'GlueDatabase', {
            catalogId: Stack.of(this).account,
            databaseInput: {
                name: glueDatabaseName,
            }
        });

        const GlueCWMetricTable = new CfnTable(this, 'GlueCWMetricTable', {
            catalogId: Stack.of(this).account,
            databaseName: glueDatabaseName,
            tableInput: {
                tableType: "EXTERNAL_TABLE",
                parameters: {
                    external: "TRUE",
                },
                storageDescriptor: {
                    columns: [
                        {
                            name: "job_name",
                            type: "string"
                        },
                        {
                            name: "job_run_id",
                            type: "string"
                        },
                        {
                            name: "cpu_executor_id",
                            type: "string"
                        },
                        {
                            name: "heap_used_executor_id",
                            type: "string"
                        },
                        {
                            name: "heap_usage_executor_id",
                            type: "string"
                        },
                        {
                            name: "all_executors",
                            type: "double"
                        },
                        {
                            name: "max_needed_executors",
                            type: "double"
                        },
                        {
                            name: "agg_gb_read",
                            type: "double"
                        },
                        {
                            name: "elapsed_minutes",
                            type: "double"
                        },
                        {
                            name: "completed_stages",
                            type: "double"
                        },
                        {
                            name: "completed_tasks",
                            type: "double"
                        },
                        {
                            name: "failed_tasks",
                            type: "double"
                        },
                        {
                            name: "stopped_tasks",
                            type: "double"
                        },
                        {
                            name: "records_read",
                            type: "double"
                        },
                        {
                            name: "diskspace_used_mb",
                            type: "double"
                        },
                        {
                            name: "driver_heap_usage",
                            type: "double"
                        },
                        {
                            name: "executor_heap_usage",
                            type: "double"
                        },
                        {
                            name: "driver_heap_used",
                            type: "double"
                        },
                        {
                            name: "executor_heap_used",
                            type: "double"
                        },
                        {
                            name: "s3_read_gb",
                            type: "double"
                        },
                        {
                            name: "s3_write_gb",
                            type: "double"
                        },
                        {
                            name: "driver_cpu",
                            type: "double"
                        },
                        {
                            name: "executor_cpu",
                            type: "double"
                        },
                        {
                            name: "streaming_records",
                            type: "double"
                        },
                        {
                            name: "streaming_batch_process_seconds",
                            type: "double"
                        },
                        {
                            name: "timestamp",
                            type: "timestamp"
                        }
                    ],
                    location: `s3://${resultBucket.bucketName}/glue-cw-metrics`,
                    inputFormat: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    outputFormat: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    compressed: false,
                    numberOfBuckets: -1,
                    serdeInfo: {
                        serializationLibrary: "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        parameters: {
                            "serialization.format": "1"
                        }
                    },
                    storedAsSubDirectories: false
                },
                partitionKeys: [
                    {
                        name: "partition_hour",
                        type: "string"
                    }
                ],
                retention: 0,
                name: "glue_cw_metrics"
            }
        });


        const GlueJobInfoTable = new CfnTable(this, 'GlueJobInfoTable', {
            catalogId: Stack.of(this).account,
            databaseName: glueDatabaseName,
            tableInput: {
                tableType: "EXTERNAL_TABLE",
                parameters: {
                    "auto.purge": "false",
                    "parquet.compression": "SNAPPY",
                    external: "TRUE",
                    transactional: "false",
                },
                storageDescriptor: {
                    columns: [
                        {
                            name: "job_run_id",
                            type: "string"
                        },
                        {
                            name: "job_name",
                            type: "string"
                        },
                        {
                            name: "is_autoscaled",
                            type: "string"
                        },
                        {
                            name: "price_mult",
                            type: "decimal(38,9)"
                        },
                        {
                            name: "job_timeout",
                            type: "int"
                        },
                        {
                            name: "exec_class",
                            type: "string"
                        },
                        {
                            name: "job_end_time",
                            type: "timestamp"
                        },
                        {
                            name: "job_runtime",
                            type: "decimal(38,9)"
                        },
                        {
                            name: "job_start_time",
                            type: "timestamp"
                        },
                        {
                            name: "num_workers",
                            type: "decimal(38,9)"
                        },
                        {
                            name: "job_state",
                            type: "string"
                        },
                        {
                            name: "job_type",
                            type: "string"
                        },
                        {
                            name: "dpu_seconds",
                            type: "decimal(38,9)"
                        },
                        {
                            name: "rate",
                            type: "decimal(38,9)"
                        },
                        {
                            name: "worker_type",
                            type: "string"
                        },
                        {
                            name: "job_version",
                            type: "string"
                        },
                        {
                            name: "run_attempt",
                            type: "decimal(38,9)"
                        },
                        {
                            name: "est_cost",
                            type: "decimal(38,36)"
                        },
                        {
                            name: "job_detail",
                            type: "string"
                        }
                    ],
                    location: `s3://${resultBucket.bucketName}/glue-job-info`,
                    inputFormat: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    outputFormat: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    compressed: false,
                    numberOfBuckets: -1,
                    serdeInfo: {
                        serializationLibrary: "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        parameters:
                            {
                                "serialization.format": "1"
                            }
                    },
                    storedAsSubDirectories: false
                },
                partitionKeys: [
                    {
                        name: "partition_hour",
                        type: "string"
                    }
                ],
                retention: 0,
                name: "glue_job_info"
            }
        });

        this.GlueDatabase.node.addDependency(role)
        GlueCWMetricTable.node.addDependency(this.GlueDatabase)
        GlueJobInfoTable.node.addDependency(this.GlueDatabase)
    }
}
