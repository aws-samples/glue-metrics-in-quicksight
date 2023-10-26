// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0.

import {NestedStack, RemovalPolicy, Stack, StackProps} from 'aws-cdk-lib';
import {Construct} from 'constructs';
import {CfnApplication} from "aws-cdk-lib/aws-sam";
import {Bucket, IBucket} from "aws-cdk-lib/aws-s3";
import {CfnDataCatalog, CfnWorkGroup} from "aws-cdk-lib/aws-athena";
import {Function, IFunction} from "aws-cdk-lib/aws-lambda";

export interface FederatedQueriesStackProps extends StackProps {
    workgroup: CfnWorkGroup,
    s3Bucket: IBucket,
}

export class FederatedQueries extends NestedStack {
    public readonly athenaFederatedCloudwatchFn: IFunction;
    public readonly athenaFederatedDynamoDBFn: IFunction;

    constructor(scope: Construct, id: string, props: FederatedQueriesStackProps) {
        super(scope, id, props);

        const { workgroup, s3Bucket } = props;

        // AWS Lambda for Athena Federated Queries into cloudwatch metrics
        const cloudwatchCatalogName= workgroup.name + "_cloudwatch"
        new CfnApplication(this, "AthenaCloudwatchConnector", {
            location: {
                applicationId: "arn:aws:serverlessrepo:us-east-1:292517598671:applications/AthenaCloudwatchMetricsConnector",
                semanticVersion: "2023.25.1"
            },
            parameters: {
                AthenaCatalogName: cloudwatchCatalogName,
                SpillBucket: s3Bucket.bucketName
            }
        })
        const cloudwatchFn = Function.fromFunctionName(this, "CloudwatchFederatedQueryFunction", cloudwatchCatalogName)

        // Create an athena catalog using the underlying lambda function as a source
        new CfnDataCatalog(this , "AthenaDataCatalogForCloudwatch", {
            name: cloudwatchCatalogName,
            type: "LAMBDA",
            parameters: {
                'function': cloudwatchFn.functionArn
            }
        })

        // AWS Lambda for Athena Federated Queries into dynamodb
        const dynamoDBCatalogName= workgroup.name + "_dynamodb"
        new CfnApplication(this, "AthenaDynamoDBConnector", {
            location: {
                applicationId: "arn:aws:serverlessrepo:us-east-1:292517598671:applications/AthenaDynamoDBConnector",
                semanticVersion: "2023.25.1"
            },
            parameters: {
                AthenaCatalogName: dynamoDBCatalogName,
                SpillBucket: s3Bucket.bucketName
            }
        })
        const dynamoDBFn = Function.fromFunctionName(this, "DynamoDBFederatedQueryFunction", dynamoDBCatalogName)

        // Create an athena catalog using the underlying lambda function as a source
        new CfnDataCatalog(this , "AthenaDataCatalogForDynamoDB", {
            name: dynamoDBCatalogName,
            type: "LAMBDA",
            parameters: {
                'function': dynamoDBFn.functionArn
            }
        })

        this.athenaFederatedCloudwatchFn = cloudwatchFn;
        this.athenaFederatedDynamoDBFn = dynamoDBFn;
    }
}