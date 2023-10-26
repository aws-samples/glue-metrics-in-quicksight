// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0.

import {NestedStack, Stack, StackProps} from 'aws-cdk-lib';
import {Construct} from 'constructs';
import {Policy, PolicyStatement, Role} from "aws-cdk-lib/aws-iam";
import * as path from "path";
import {TEMPLATE_DIR} from "../util/project";
import {CfnInclude} from "aws-cdk-lib/cloudformation-include";
import {IBucket} from "aws-cdk-lib/aws-s3";
import {CfnWorkGroup} from "aws-cdk-lib/aws-athena";
import {IFunction} from "aws-cdk-lib/aws-lambda";

export interface QuicksightProps extends StackProps {
    readonly workgroup: CfnWorkGroup
    readonly quicksightDatasetIdMetrics: string,
    readonly quicksightDatasetIdInfo: string,
    readonly resultBucket: IBucket,
    readonly quicksightUsername: string,
    readonly glueDatabaseName: string
    readonly athenaFederatedCloudwatchFn: IFunction;
    readonly athenaFederatedDynamoDBFn: IFunction;
}

export class QuicksightStack extends NestedStack {
    constructor(scope: Construct, id: string, props: QuicksightProps) {
        super(scope, id, props);

        const {
            workgroup,
            quicksightDatasetIdMetrics,
            quicksightDatasetIdInfo,
            resultBucket,
            quicksightUsername,
            glueDatabaseName,
            athenaFederatedCloudwatchFn,
            athenaFederatedDynamoDBFn
        } = props

        const quicksightRole = Role.fromRoleArn(this, 'QuicksightRole', `arn:aws:iam::${Stack.of(this).account}:role/service-role/aws-quicksight-service-role-v0`);
        if (!quicksightRole) throw Error("Quicksight managed service role needs to be created, have you manually created an quicksight account yet?")

        resultBucket.grantReadWrite(quicksightRole);

        quicksightRole.attachInlinePolicy(new Policy(this, 'QuicksightPolicy', {
            statements: [
                new PolicyStatement({
                    actions: [
                        "athena:ListDataCatalogs",
                    ],
                    resources: ["*"]
                }),
                new PolicyStatement({
                    actions: [
                        "athena:StartQueryExecution",
                        "athena:GetQueryExecution",
                        "athena:GetQueryResultsStream",
                    ],
                    resources: [`arn:aws:athena:${Stack.of(this).region}:${Stack.of(this).account}:workgroup/${workgroup.name}`]
                }),
                new PolicyStatement({
                    actions: [
                        "glue:GetTable",
                        "glue:GetPartitions"
                    ],
                    resources: [
                        `arn:aws:glue:${Stack.of(this).region}:${Stack.of(this).account}:catalog`,
                        `arn:aws:glue:${Stack.of(this).region}:${Stack.of(this).account}:database/${glueDatabaseName}`,
                        `arn:aws:glue:${Stack.of(this).region}:${Stack.of(this).account}:table/${glueDatabaseName}/glue_cw_metrics`,
                        `arn:aws:glue:${Stack.of(this).region}:${Stack.of(this).account}:table/${glueDatabaseName}/glue_job_info`,
                    ]
                }),
            ]
        }));

        athenaFederatedCloudwatchFn.grantInvoke(quicksightRole)
        athenaFederatedDynamoDBFn.grantInvoke(quicksightRole)

        let stack = new CfnInclude(this, 'QuicksightStack', {
            templateFile: path.resolve(TEMPLATE_DIR, "glue_quicksight_cfn.json"),
            parameters: {
                "AthenaWorkgroupName": workgroup.name,
                "QuicksightUsername": quicksightUsername,
                "QuicksightDatasetIdMetrics": quicksightDatasetIdMetrics,
                "QuicksightDatasetIdInfo": quicksightDatasetIdInfo,
                "QuicksightGlueDatabaseName": glueDatabaseName
            }
        })

        stack.node.addDependency(quicksightRole)
    }
}