#!/usr/bin/env python3
import os

import aws_cdk as cdk

from bulk_load_dynamodb import BulkLoadDynamodbStack


app = cdk.App()
environment = app.node.try_get_context("environment")
BulkLoadDynamodbStack(
    app,
    "BulkLoadDynamodbStack",
    env=cdk.Environment(region=environment["AWS_REGION"]),
    environment=environment,
)
app.synth()
