// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0.

import {NestedStack, RemovalPolicy, StackProps} from "aws-cdk-lib";
import {Construct} from "constructs";
import {Bucket, BucketEncryption} from "aws-cdk-lib/aws-s3";

export interface StorageProps extends StackProps {
    bucketName?: string;
}

/**
 * Storage stack creates an S3 bucket used for storing the results of the Athena queries,
 * including any spill results from the federated queries. The bucket is also used as a
 * data source for Quicksight (through Athena).
 */
export class Storage extends NestedStack {
    public readonly resultsBucket: Bucket;

    constructor(scope: Construct, id: string, props?: StorageProps) {
        super(scope, id, props);

        const bucketName = props?.bucketName

        this.resultsBucket = new Bucket(this, 'ResultsBucket', {
            removalPolicy: RemovalPolicy.DESTROY,
            autoDeleteObjects: true,
            bucketName: bucketName,
            enforceSSL: true,
            encryption: BucketEncryption.S3_MANAGED
        })
    }
}
