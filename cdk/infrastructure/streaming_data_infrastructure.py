import aws_cdk as core
from aws_cdk import Stack  # Duration,; aws_sqs as sqs,
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kinesis as kinesis
from aws_cdk import aws_kinesisfirehose as firehose
from aws_cdk import aws_lambda
from aws_cdk import aws_s3 as s3
from constructs import Construct

PREFIX = "test"
DATABASE_NAME = "cricket_stream"

"""
import boto3

glue = boto3.client('glue')

response = glue.create_classifier(
    JsonClassifier={
        'Name': 'JsonClassifier',
        'JsonPath': '\n'
    }
)

"""


class StreamingDataInfrastructure(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        kinesis_stream = kinesis.Stream(self, "KinesisStream")

        cricket_stream_host_bucket = s3.Bucket(self, "CricketStreamHostBucket")

        lambda_function = aws_lambda.Function(
            self,
            "TransformationFunction",
            code=aws_lambda.Code.from_asset(
                "infrastructure/data_transformation_lambda.zip"
            ),
            handler="data_transformation_lambda.handler",
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            timeout=core.Duration.seconds(30),
        )

        lambda_invoke_policy = iam.PolicyStatement(
            actions=["lambda:InvokeFunction"],
            resources=[lambda_function.function_arn],
        )

        firehose_role = iam.Role(
            self,
            "FirehoseRole",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("firehose.amazonaws.com"),
                iam.ServicePrincipal("lambda.amazonaws.com"),
            ),
        )

        firehose_role.add_to_policy(lambda_invoke_policy)

        firehose_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonKinesisFullAccess")
        )

        cricket_stream_host_bucket.grant_write(firehose_role)

        lambda_function.add_permission(
            "FirehoseAccess",
            principal=iam.ServicePrincipal("firehose.amazonaws.com"),
        )

        firehose.CfnDeliveryStream(
            self,
            "FirehoseStream",
            delivery_stream_type="KinesisStreamAsSource",
            kinesis_stream_source_configuration=firehose.CfnDeliveryStream.KinesisStreamSourceConfigurationProperty(
                kinesis_stream_arn=kinesis_stream.stream_arn,
                role_arn=firehose_role.role_arn,
            ),
            extended_s3_destination_configuration=firehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
                bucket_arn=cricket_stream_host_bucket.bucket_arn,
                buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                    interval_in_seconds=60, size_in_m_bs=50
                ),
                compression_format="UNCOMPRESSED",
                role_arn=firehose_role.role_arn,
                prefix=f"{PREFIX}"
                + "/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/",
                error_output_prefix="error/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/!{firehose:error-output-type}",
                processing_configuration=firehose.CfnDeliveryStream.ProcessingConfigurationProperty(
                    enabled=True,
                    processors=[
                        firehose.CfnDeliveryStream.ProcessorProperty(
                            type="Lambda",
                            parameters=[
                                firehose.CfnDeliveryStream.ProcessorParameterProperty(
                                    parameter_name="LambdaArn",
                                    parameter_value=lambda_function.function_arn,
                                ),
                            ],
                        )
                    ],
                ),
            ),
        )

        glue.CfnDatabase(
            self,
            "CricketStream",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(name=DATABASE_NAME),
        )

        cricket_glue_role = iam.Role(
            self,
            "CricketCrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
        )

        cricket_glue_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSGlueServiceRole"
            )
        )

        cricket_stream_host_bucket.grant_read(cricket_glue_role)

        glue.CfnCrawler(
            self,
            "CricketCrawler",
            database_name=DATABASE_NAME,
            role=cricket_glue_role.role_arn,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{cricket_stream_host_bucket.bucket_name}/{PREFIX}/"
                    )
                ]
            ),
            schedule=glue.CfnCrawler.ScheduleProperty(
                schedule_expression="cron(0 7 * * ? *)"  # run at 7am UTC everyday
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                delete_behavior="LOG", update_behavior="UPDATE_IN_DATABASE"
            ),
        )
