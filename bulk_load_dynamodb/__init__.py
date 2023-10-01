import json

from aws_cdk import (
    BundlingOptions,
    Duration,
    RemovalPolicy,
    Stack,
    aws_cloudwatch as cloudwatch,
    aws_dynamodb as dynamodb,
    aws_lambda as _lambda,
    aws_lambda_event_sources as _lambda_event_sources,
    aws_logs as logs,
    aws_s3 as s3,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subs,
    aws_sqs as sqs,
    aws_s3_deployment as s3_deploy,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
)
from constructs import Construct


class BulkLoadDynamodbStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, environment: dict, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.dynamodb_table = dynamodb.Table(
            self,
            "DynamodbTable",
            table_name=environment["DYNAMODB_TABLE_NAME"],
            partition_key=dynamodb.Attribute(
                name="pk", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(name="sk", type=dynamodb.AttributeType.STRING),
            stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.s3_bucket = s3.Bucket(
            self,
            "S3Bucket",
            bucket_name=environment["S3_BUCKET_NAME"],
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )
        data_files = s3_deploy.BucketDeployment(
            self,
            "data_files",
            destination_bucket=self.s3_bucket,
            destination_key_prefix="data/",  # hard coded
            sources=[s3_deploy.Source.asset("./data")],  # hard coded
            prune=True,  ### it seems that delete Lambda uses a different IAM role
            retain_on_delete=False,
            memory_limit=1024,  # need more RAM for large files
        )

        self.sns_topic = sns.Topic(
            self, "SnsTopic", topic_name=environment["SNS_TOPIC_NAME"]
        )

        self.dlq_for_sns_messages = sqs.Queue(
            self,
            "DlqForSnsMessages",
            queue_name=environment["SQS_DLQ_NAME"],
            retention_period=Duration.days(7),
            removal_policy=RemovalPolicy.DESTROY,
        )
        dlq_for_sns_messages = sqs.DeadLetterQueue(
            max_receive_count=3, queue=self.dlq_for_sns_messages
        )
        self.queue_for_sns_messages = sqs.Queue(
            self,
            "QueueForSnsMessages",
            queue_name=environment["SQS_QUEUE_NAME"],
            retention_period=Duration.days(4),
            visibility_timeout=Duration.seconds(
                environment["SQS_QUEUE_VISIBILITY_TIMEOUT_SECONDS"]
            ),
            dead_letter_queue=dlq_for_sns_messages,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.downstream_dlq = sqs.Queue(
            self,
            "DownstreamDLQ",
            queue_name=environment["SQS_DLQ_NAME_DOWNSTREAM"],
            retention_period=Duration.days(7),
            removal_policy=RemovalPolicy.DESTROY,
        )
        downstream_dlq = sqs.DeadLetterQueue(
            max_receive_count=3, queue=self.downstream_dlq
        )
        self.downstream_queue = sqs.Queue(
            self,
            "DownstreamQueue",
            queue_name=environment["SQS_QUEUE_NAME_DOWNSTREAM"],
            retention_period=Duration.days(4),
            visibility_timeout=Duration.seconds(
                environment["SQS_QUEUE_VISIBILITY_TIMEOUT_SECONDS"]  # reuse
            ),
            dead_letter_queue=downstream_dlq,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.split_data_lambda = _lambda.Function(
            self,
            "SplitDataLambda",
            function_name="split_data",  # hard coded
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "lambda_code/split_data_lambda",
                # exclude=[".venv/*"],  # seems to no longer do anything if use BundlingOptions
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        " && ".join(
                            [
                                "pip install -r requirements.txt -t /asset-output",
                                "cp handler.py /asset-output",  # need to cp instead of mv
                            ]
                        ),
                    ],
                ),
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(60),
            memory_size=1024,
            environment={
                "S3_BUCKET_NAME": environment["S3_BUCKET_NAME"],
                "PARTITION_ROW_COUNT": json.dumps(environment["PARTITION_ROW_COUNT"]),
            },
            log_retention=logs.RetentionDays.ONE_MONTH,
        )
        self.load_data_lambda = _lambda.Function(
            self,
            "LoadDataLambda",
            function_name="load_data",  # hard coded
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "lambda_code/load_data_lambda",
                # exclude=[".venv/*"],  # seems to no longer do anything if use BundlingOptions
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_9.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        " && ".join(
                            [
                                "pip install -r requirements.txt -t /asset-output",
                                "cp handler.py /asset-output",  # need to cp instead of mv
                            ]
                        ),
                    ],
                ),
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(
                900
            ),  # depends on whether DynamoDB table is cold or warm
            memory_size=1024,
            environment={
                "S3_BUCKET_NAME": environment["S3_BUCKET_NAME"],
                "DYNAMODB_TABLE_NAME": environment["DYNAMODB_TABLE_NAME"],
            },
            log_retention=logs.RetentionDays.ONE_MONTH,
        )
        self.sort_runtimes_lambda = _lambda.Function(
            self,
            "SortRuntimesLambda",
            function_name="sort_runtimes",  # hard coded
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.InlineCode(
                code="def lambda_handler(event, context): return sorted(event)"
            ),
            handler="index.lambda_handler",
            timeout=Duration.seconds(1),  # should be instantaneous
            memory_size=256,  # needs little memory
            log_retention=logs.RetentionDays.ONE_MONTH,
        )

        # build Step Function definition
        split_data = sfn_tasks.LambdaInvoke(
            self,
            "split_data",
            lambda_function=self.split_data_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "key": "data/dynamodb_data_balanced_10k_with_amount.csv"
                }  # or use data/dynamodb_data_hotkey.csv
            ),
            payload_response_only=True,  # don't want Lambda invocation metadata
            task_timeout=sfn.Timeout.duration(self.split_data_lambda.timeout),
            retry_on_service_exceptions=False,
        )
        load_data = sfn_tasks.LambdaInvoke(
            self,
            "load_data",
            lambda_function=self.load_data_lambda,
            payload=sfn.TaskInput.from_object(
                {"key": sfn.JsonPath.string_at("$")}  # hard coded
            ),
            payload_response_only=True,  # don't want Lambda invocation metadata
            task_timeout=sfn.Timeout.duration(self.load_data_lambda.timeout),
            retry_on_service_exceptions=False,
        )
        sort_runtimes = sfn_tasks.LambdaInvoke(
            self,
            "sort_runtimes",
            lambda_function=self.sort_runtimes_lambda,
            payload_response_only=True,  # don't want Lambda invocation metadata
            task_timeout=sfn.Timeout.duration(self.sort_runtimes_lambda.timeout),
            retry_on_service_exceptions=False,
        )
        map_state = sfn.Map(
            self, "parallel_load_data", max_concurrency=10, items_path="$"
        )
        map_state.iterator(load_data)
        sfn_definition = split_data.next(map_state).next(sort_runtimes)
        self.state_machine = sfn.StateMachine(
            self,
            "parallel_dynamodb_load",
            state_machine_name="parallel_dynamodb_load",  # hard coded
            definition=sfn_definition,
        )

        self.update_dynamodb_table_lambda = _lambda.Function(
            self,
            "UpdateDynamodbTableLambda",
            function_name="update_dynamodb_table",  # hard coded
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "lambda_code/update_dynamodb_table_lambda",
                exclude=[".venv/*"],
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(1),  # should be instantaneous
            memory_size=256,  # needs little memory
            environment={
                "DYNAMODB_TABLE_NAME": environment["DYNAMODB_TABLE_NAME"],
            },
            log_retention=logs.RetentionDays.ONE_MONTH,
        )
        self.update_downstream_service_lambda = _lambda.Function(
            self,
            "UpdateDownstreamServiceLambda",
            function_name="update_downstream_service",  # hard coded
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "lambda_code/update_downstream_service_lambda",
                exclude=[".venv/*"],
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(1),  # should be instantaneous
            memory_size=256,  # needs little memory
            environment={
                "SQS_QUEUE_NAME_DOWNSTREAM": environment["SQS_QUEUE_NAME_DOWNSTREAM"]
            },
            log_retention=logs.RetentionDays.ONE_MONTH,
        )

        # connect AWS resources together
        self.state_machine_alarm = cloudwatch.Alarm(
            self,
            "StateMachineAlarm",
            alarm_name=f"{self.state_machine.state_machine_name}-alarm",
            metric=self.state_machine.metric_failed(
                statistic="sum", period=Duration.minutes(1)  # hard coded
            ),
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
            threshold=0,
            evaluation_periods=1,
            treat_missing_data=cloudwatch.TreatMissingData.IGNORE,
        )
        self.sns_topic.add_subscription(
            topic_subscription=sns_subs.SqsSubscription(
                self.queue_for_sns_messages,
                raw_message_delivery=True,
            )
        )
        sqs_to_lambda = _lambda_event_sources.SqsEventSource(
            self.queue_for_sns_messages, batch_size=1
        )
        self.update_dynamodb_table_lambda.add_event_source(source=sqs_to_lambda)
        self.update_downstream_service_lambda.add_event_source(
            _lambda_event_sources.DynamoEventSource(
                self.dynamodb_table,
                starting_position=_lambda.StartingPosition.LATEST,
                batch_size=1,  # hard coded
                retry_attempts=1,  # hard coded
                on_failure=_lambda_event_sources.SqsDlq(queue=self.downstream_dlq),
                # max_batching_window=Duration.seconds(0),  # hard coded
                # filters=[{"event_name": _lambda.FilterRule.is_equal("MODIFY")}]
            )
        )
        self.update_dynamodb_table_alarm = cloudwatch.Alarm(
            self,
            "UpdateDynamodbTableAlarm",
            alarm_name=f"{self.update_dynamodb_table_lambda.function_name}-alarm",
            metric=self.update_dynamodb_table_lambda.metric_errors(
                statistic="sum", period=Duration.minutes(1)  # hard coded
            ),
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
            threshold=0,
            evaluation_periods=1,
            treat_missing_data=cloudwatch.TreatMissingData.IGNORE,
        )
        self.update_downstream_service_alarm = cloudwatch.Alarm(
            self,
            "UpdateDownstreamServiceAlarm",
            alarm_name=f"{self.update_downstream_service_lambda.function_name}-alarm",
            metric=self.update_downstream_service_lambda.metric_errors(
                statistic="sum", period=Duration.minutes(1)  # hard coded
            ),
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
            threshold=0,
            evaluation_periods=1,
            treat_missing_data=cloudwatch.TreatMissingData.IGNORE,
        )
        self.dlq_for_sns_messages_alarm = cloudwatch.Alarm(
            self,
            "DlqForSnsMessagesAlarm",
            alarm_name=f"{self.dlq_for_sns_messages.queue_name}-alarm",
            metric=self.dlq_for_sns_messages.metric(
                "ApproximateNumberOfMessagesVisible",
                statistic="sum",
                period=Duration.minutes(1),  # hard coded
            ),
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
            threshold=0,
            evaluation_periods=1,
            treat_missing_data=cloudwatch.TreatMissingData.IGNORE,
        )
        self.downstream_dlq_alarm = cloudwatch.Alarm(
            self,
            "DownstreamDlqAlarm",
            alarm_name=f"{self.downstream_dlq.queue_name}-alarm",
            metric=self.downstream_dlq.metric(
                "ApproximateNumberOfMessagesVisible",
                statistic="sum",
                period=Duration.minutes(1),  # hard coded
            ),
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
            threshold=0,
            evaluation_periods=1,
            treat_missing_data=cloudwatch.TreatMissingData.IGNORE,
        )
        self.s3_bucket.grant_read_write(self.split_data_lambda)
        self.s3_bucket.grant_read_write(self.load_data_lambda)
        self.dynamodb_table.grant_write_data(self.load_data_lambda)
        self.dynamodb_table.grant_write_data(self.update_dynamodb_table_lambda)
        self.downstream_queue.grant_send_messages(self.update_downstream_service_lambda)
