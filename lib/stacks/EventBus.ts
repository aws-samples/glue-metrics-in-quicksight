// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0.

import {NestedStack, Stack, StackProps} from "aws-cdk-lib";
import {Rule, Schedule} from "aws-cdk-lib/aws-events";
import {SfnStateMachine} from "aws-cdk-lib/aws-events-targets";
import {Construct} from "constructs";
import {IStateMachine} from "aws-cdk-lib/aws-stepfunctions";

export interface EventBusProps extends StackProps {
    readonly stateMachine: IStateMachine;
}
export class EventBus extends NestedStack {

    constructor(scope: Construct, id: string, props: EventBusProps) {
        super(scope, id, props);

        const { stateMachine } = props;

        const cronRule = new Rule(this, 'CronRule', {
            schedule: Schedule.cron({hour: '*', minute: '1'})
        })

        cronRule.addTarget(new SfnStateMachine(stateMachine))
    }
}
