#!/usr/bin/env node
// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import {QuicksightStack} from "../lib/stacks/Quicksight";
import {Database} from "../lib/stacks/Database";
import {EventBus} from "../lib/stacks/EventBus";
import {FederatedQueries} from "../lib/stacks/FederatedQueries";
import {AthenaWorkgroupAndQueries} from "../lib/stacks/Athena";
import {StepFunctions} from "../lib/stacks/StepFunction";
import {Storage} from "../lib/stacks/Storage";
import {GlueTables} from "../lib/stacks/GlueTables";
import {Aspects, CfnParameter, Stack, StackProps} from "aws-cdk-lib";
import {Construct} from "constructs";
import {AwsSolutionsChecks, NagSuppressions} from "cdk-nag";


const app = new cdk.App();

class GlueMetricsInQuicksight extends Stack {
    constructor(scope: Construct, id: string, props?: StackProps) {
        super(scope, id, props);

        const config = {
            bucketName: new CfnParameter(this, 'BucketName', { description: 'Name of the S3 bucket to create for storing query results'}),
            workgroupName: new CfnParameter(this, 'WorkgroupName', { description: 'Name of the Athena Workgroup to create for the pipeline', default: 'glue_pipeline_workgroup'}),
            glueDatabaseName: new CfnParameter(this, 'GlueDatabaseName', { description: 'Name of the Glue Database to create for the pipeline', default: 'glue_info_db'}),
            quicksightDatasetIdMetrics: new CfnParameter(this, 'QuicksightMetricsDatasetId', { description: 'Quicksight Dataset ID for the metrics dataset, used so step functions knows where in quicksight to update', default: 'QuicksightMetricsDatasetId'}),
            quicksightDatasetIdInfo: new CfnParameter(this, 'QuicksightInfoDatasetId', { description: 'Quicksight Dataset ID for the info dataset, used so step functions knows where in quicksight to update', default: 'QuicksightInfoDatasetId'}),
            dynamodbTableName: new CfnParameter(this, 'DynamoDBTableName', { description: 'Name of the DynamoDB table to create for the pipeline', default: 'glue_metadata_table'}),
            quicksightUsername: new CfnParameter(this, 'QuicksightUsername', { description: 'Username of the Quicksight user, must be created prior to to view dashboard'}),
        }

        const storageStack = new Storage(this, 'StorageStack', {
            bucketName: config.bucketName.valueAsString,
        })
        const databaseStack = new Database(this, 'DatabaseStack', {
            tableName: config.dynamodbTableName.valueAsString,
        })
        const glueTableStack = new GlueTables(this, 'GlueTablesStack', {
            glueDatabaseName: config.glueDatabaseName.valueAsString,
            resultBucket: storageStack.resultsBucket
        })
        const athenaStack = new AthenaWorkgroupAndQueries(this, 'AthenaStack', {
            workgroupName: config.workgroupName.valueAsString,
            s3Bucket: storageStack.resultsBucket,
        });
        const federatedQueriesStack = new FederatedQueries(this, 'FederatedQueriesStack', {
            workgroup: athenaStack.workgroup,
            s3Bucket: storageStack.resultsBucket,
        });
        const stepFunctionsStack = new StepFunctions(this, 'StepFunctionsStack', {
            ddbTable: databaseStack.database,
            quicksightDatasetIdInfo: config.quicksightDatasetIdInfo.valueAsString,
            quicksightDatasetIdMetrics: config.quicksightDatasetIdMetrics.valueAsString,
            resultsBucket: storageStack.resultsBucket,
            workgroup: athenaStack.workgroup,
            glueDatabaseName: config.glueDatabaseName.valueAsString,
            athenaFederatedCloudwatchFn: federatedQueriesStack.athenaFederatedCloudwatchFn,
            athenaFederatedDynamoDBFn: federatedQueriesStack.athenaFederatedDynamoDBFn,
        })
        const eventConsumerStack = new EventBus(this, 'EventConsumersStack', {
            stateMachine: stepFunctionsStack.stateMachine,
        });
        const quicksightStack = new QuicksightStack(this, 'QuicksightStack', {
            workgroup: athenaStack.workgroup,
            quicksightUsername: config.quicksightUsername.valueAsString,
            quicksightDatasetIdInfo: config.quicksightDatasetIdInfo.valueAsString,
            quicksightDatasetIdMetrics: config.quicksightDatasetIdMetrics.valueAsString,
            resultBucket: storageStack.resultsBucket,
            glueDatabaseName: config.glueDatabaseName.valueAsString,
            athenaFederatedCloudwatchFn: federatedQueriesStack.athenaFederatedCloudwatchFn,
            athenaFederatedDynamoDBFn: federatedQueriesStack.athenaFederatedDynamoDBFn,
        });
        quicksightStack.node.addDependency(athenaStack)

        // AWS Solutions suppressions
        NagSuppressions.addResourceSuppressionsByPath(
            this,
            '/GlueMetricsInQuicksight/StorageStack/ResultsBucket/Resource',
            [{ id: 'AwsSolutions-S1', reason: 'Server access logs on S3 bucket not appropriate for sample solution' }]
        );

        NagSuppressions.addResourceSuppressionsByPath(
            this,
            '/GlueMetricsInQuicksight/GlueTablesStack/GlueBlogRole/Resource',
            [{ id: 'AwsSolutions-IAM4', reason: 'Using the Glue managed policy for the Glue service role is appropriate for a sample solution' }]
        );

        NagSuppressions.addResourceSuppressionsByPath(
            this,
            '/GlueMetricsInQuicksight/GlueTablesStack/GlueBlogRole/Resource',
            [{ id: 'AwsSolutions-IAM4', reason: 'Using the Glue managed policy for the Glue service role is appropriate' }]
        );

        NagSuppressions.addResourceSuppressionsByPath(
            this,
            '/GlueMetricsInQuicksight/AthenaStack/AthenaWorkgroup',
            [{ id: 'AwsSolutions-ATH1', reason: 'Bucket used by Athena workgroup is encrypted with S3 managed key' }]
        );

        NagSuppressions.addResourceSuppressionsByPath(
            this,
            '/GlueMetricsInQuicksight/StepFunctionsStack/GlueJobConsumer/ServiceRole/Resource',
            [{ id: 'AwsSolutions-IAM4', reason: 'AWS Lambda Execution Role (for GlueJobConsumer) has appropriate managed policies for sample solution' }]
        );

        NagSuppressions.addResourceSuppressionsByPath(
            this,
            '/GlueMetricsInQuicksight/StepFunctionsStack/GlueJobConsumer/ServiceRole/Resource',
            [{ id: 'AwsSolutions-IAM4', reason: 'AWS Lambda Execution Role (for GlueJobConsumer) has appropriate managed policies for sample solution' }]
        );

        NagSuppressions.addResourceSuppressionsByPath(
            this,
            '/GlueMetricsInQuicksight/DatabaseStack/Database/Resource',
            [{ id: 'AwsSolutions-DDB3', reason: 'DynamoDB Table does not need Point-in-time recovery enabled for sample solution' }]
        );

        NagSuppressions.addResourceSuppressionsByPath(
            this,
            '/GlueMetricsInQuicksight/StepFunctionsStack/GlueJobConsumer/ServiceRole/DefaultPolicy/Resource',
            [{ id: 'AwsSolutions-IAM5', reason: 'Jobs are created at runtime with no static referencable ARN (see Glue Docs), hence the most we can limit is account and region' }]
        );

        NagSuppressions.addResourceSuppressionsByPath(
            this,
            '/GlueMetricsInQuicksight/StepFunctionsStack/StateMachine/Role/DefaultPolicy/Resource',
            [{ id: 'AwsSolutions-IAM5', reason: 'Manually reviewed Step functions policy, its least privilege based upon the information known at deployment time' }]
        );

        NagSuppressions.addResourceSuppressionsByPath(
            this,
            '/GlueMetricsInQuicksight/StepFunctionsStack/StateMachine/Resource',
            [{ id: 'AwsSolutions-SF2', reason: 'Enabling x-ray is out of scope for this sample solution' }]
        );

        NagSuppressions.addResourceSuppressionsByPath(
            this,
            '/GlueMetricsInQuicksight/QuicksightStack/QuicksightPolicy/Resource',
            [{ id: 'AwsSolutions-IAM5', reason: 'List permission requires star' }]
        );

        NagSuppressions.addResourceSuppressionsByPath(
            this,
            '/GlueMetricsInQuicksight/QuicksightStack/QuicksightRole/PolicyGlueMetricsInQuicksightQuicksightStackQuicksightRole916D490B/Resource',
            [{ id: 'AwsSolutions-IAM5', reason: 'Manually reviewed policy and resource stars are within reason with all available information at deployment time' }]
        );
    }
}

new GlueMetricsInQuicksight(app, 'GlueMetricsInQuicksight');
Aspects.of(app).add(new AwsSolutionsChecks({verbose: true}))