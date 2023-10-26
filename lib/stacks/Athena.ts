// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

import {NestedStack, Stack, StackProps} from 'aws-cdk-lib';
import {Construct} from 'constructs';
import {IBucket} from "aws-cdk-lib/aws-s3";
import {CfnWorkGroup} from "aws-cdk-lib/aws-athena";

export interface AthenaStackProps extends StackProps {
    workgroupName: string,
    s3Bucket: IBucket,
}

export class AthenaWorkgroupAndQueries extends NestedStack {
    public readonly workgroup: CfnWorkGroup;

    constructor(scope: Construct, id: string, props: AthenaStackProps) {
        super(scope, id, props);

        const { workgroupName, s3Bucket } = props;

        const workgroup = new CfnWorkGroup(this, 'AthenaWorkgroup', {
            name: workgroupName,
            description: 'Athena WorkGroup for Glue Info Pipeline',
            state: 'ENABLED',
            recursiveDeleteOption: true,
            workGroupConfiguration: {
                enforceWorkGroupConfiguration: true,
                publishCloudWatchMetricsEnabled: true,
                resultConfiguration: {
                    outputLocation: `s3://${s3Bucket.bucketName}/athena-results/`,
                },
            },
        });
        workgroup.node.addDependency(s3Bucket);
        this.workgroup = workgroup;
    }
}
