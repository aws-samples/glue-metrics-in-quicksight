// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0.

import {NestedStack, RemovalPolicy, Stack, StackProps} from "aws-cdk-lib";
import {AttributeType, BillingMode, Table} from "aws-cdk-lib/aws-dynamodb";
import {Construct} from "constructs";

export interface DatabaseStackProps extends StackProps {
    tableName: string
}
export class Database extends NestedStack {
    public database: Table;
    constructor(scope: Construct, id: string, props: DatabaseStackProps) {
        super(scope, id, props);

        const database = new Table(this, 'Database', {
            partitionKey: {
                name: "glue_id",
                type: AttributeType.STRING,
            },
            timeToLiveAttribute: "ttl",
            billingMode: BillingMode.PAY_PER_REQUEST,
            tableName: props.tableName,
            removalPolicy: RemovalPolicy.DESTROY,
        })

        this.database = database
    }
}
