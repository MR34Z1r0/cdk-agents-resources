from aws_cdk import (
    Stack,
    RemovalPolicy,
    Duration,
    aws_lambda_event_sources as lambda_event_sources,
    aws_lambda as _lambda,
    aws_dynamodb as dynamodb,
    aws_s3 as s3,
    aws_sqs as sqs,
    aws_iam as iam,
    aws_secretsmanager as secretsmanager,
    aws_s3_notifications as s3n,
    CfnOutput
)
from constructs import Construct
from cdk_aje_libs.builders.resource_builder import ResourceBuilder
from cdk_aje_libs.models.configs import *
from cdk_aje_libs.constants.environments import Environments
from constants.paths import Paths
import os
from dotenv import load_dotenv
import urllib.parse
from cdk_aje_libs.constants.project_config import ProjectConfig

class CdkAgentsResourcesStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, project_config: ProjectConfig, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)         
        self.PROJECT_CONFIG = project_config        
        self.builder = ResourceBuilder(self, self.PROJECT_CONFIG)
        self.Paths = Paths(project_config.app_config)

        #Import secrets
        self.secret_bot = self.builder.import_secret(f"{self.PROJECT_CONFIG.environment.value}/{self.PROJECT_CONFIG.project_name}/{self.PROJECT_CONFIG.app_config['secret_name']}")

        # Create all resources first
        self.create_dynamodb_tables()
        self.create_sqs_queues()
        self.create_s3_bucket_process()
        
        # Create Lambda functions with proper dependencies
        self.create_lambda_process()
        
        # Configure integrations correctly to avoid circular dependencies
        self.configure_integrations()
        #self.create_lambda_bot()

    def create_dynamodb_tables(self):
        # Create lote table
        dynamodb_config = DynamoDBConfig(
            table_name="lote",
            partition_key="LOTE_ID",  
            partition_key_type=dynamodb.AttributeType.STRING,
        )
        self.dynamodb_table_lote = self.builder.build_dynamodb_table(dynamodb_config)

        # Create metadata table
        dynamodb_config = DynamoDBConfig(
            table_name="metadata",
            partition_key="DOCUMENT_ID",  
            partition_key_type=dynamodb.AttributeType.STRING,
        )
        self.dynamodb_table_metadata = self.builder.build_dynamodb_table(dynamodb_config)
  
        # Create structure table
        dynamodb_config = DynamoDBConfig(
            table_name="structure",
            partition_key="DOCUMENT_STRUCTURE_ID",  
            partition_key_type=dynamodb.AttributeType.STRING,
        )
        self.dynamodb_table_structure = self.builder.build_dynamodb_table(dynamodb_config)

    def create_sqs_queues(self):
        # Create main queue first
        sqs_config = SQSConfig(  
            queue_name="process",
            visibility_timeout=Duration.seconds(30),
            retention_period=Duration.days(14)
        )        
        self.sqs_process = self.builder.build_sqs_queue(sqs_config)
        
        # Create DLQ that refers to the main queue
        sqs_config = SQSConfig(  
            queue_name="process-dlq",
            retention_period=Duration.days(14),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=2,
                queue=self.sqs_process
            )
        )
        self.sqs_process_dlq = self.builder.build_sqs_queue(sqs_config)

    def create_s3_bucket_process(self):
        s3_config = S3Config(
            bucket_name="process",
            versioned=False,
            removal_policy=RemovalPolicy.DESTROY,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL
        )

        self.s3_bucket_process = self.builder.build_s3_bucket(s3_config)
        
        # Add permissions for S3 to send messages to SQS (do this right after bucket creation)
        self.sqs_process.add_to_resource_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["sqs:SendMessage"],
                principals=[iam.ServicePrincipal("s3.amazonaws.com")],
                resources=[self.sqs_process.queue_arn],
                conditions={
                    "ArnLike": {
                        "aws:SourceArn": self.s3_bucket_process.bucket_arn
                    }
                }
            )
        )
        
    def create_lambda_process(self):
        lambda_layer_pillow = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "Layer-pillow",
            layer_version_arn=f"arn:aws:lambda:us-east-1:510543735161:layer:pillow_layer:1",
        )
        
        lambda_layer_pytz = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "Layer-pytz",
            layer_version_arn=f"arn:aws:lambda:us-east-1:510543735161:layer:pytz_layer:1",
        )

        lambda_config = LambdaConfig(
            function_name="process",
            handler="lambda_function.lambda_handler",
            code_path=f"{self.Paths.LOCAL_ARTIFACTS_LAMBDA_CODE}/process/",
            runtime=_lambda.Runtime.PYTHON_3_11,
            timeout=Duration.seconds(30),
            environment={
                "ENVIRONMENT": self.PROJECT_CONFIG.environment.value,
                "S3_BUCKET": self.s3_bucket_process.bucket_name,
                "DYNAMODB_LOTE_TABLE": self.dynamodb_table_lote.table_name,
                "DYNAMODB_METADATA_TABLE": self.dynamodb_table_metadata.table_name,
                "DYNAMODB_STRUCTURE_TABLE": self.dynamodb_table_structure.table_name,
                "SQS_QUEUE_URL": self.sqs_process.queue_url,                
            },
            layers=[lambda_layer_pillow, lambda_layer_pytz]
        )
        self.lambda_process = self.builder.build_lambda_function(lambda_config)
        
        # Grant permissions to lambda right after creation
        self.s3_bucket_process.grant_read_write(self.lambda_process)
        self.dynamodb_table_lote.grant_read_write_data(self.lambda_process)
        self.dynamodb_table_metadata.grant_read_write_data(self.lambda_process)
        self.dynamodb_table_structure.grant_read_write_data(self.lambda_process)

    def configure_integrations(self):
        """
        Configure integrations between resources without creating circular dependencies
        """
        # 1. Set up the event source mapping for Lambda to process SQS messages
        event_source = lambda_event_sources.SqsEventSource(
            self.sqs_process,
            batch_size=10,
            max_concurrency=2
        )
        self.lambda_process.add_event_source(event_source)
        
        # 2. Configure S3 bucket notifications using S3 Notifications construct
        # This avoids circular dependencies by creating a custom resource
        self.s3_bucket_process.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(self.sqs_process),
            s3.NotificationKeyFilter(prefix="KENDRA_LLM/")
        )

    def create_lambda_bot(self):
        lambda_layer_powertools = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "BotLayerPowertools",
            layer_version_arn="arn:aws:lambda:us-east-1:017000801446:layer:AWSLambdaPowertoolsPythonV3:1"
        )
       
        lambda_config = LambdaConfig(
            function_name="bot",
            handler="bot/lambda_function.lambda_handler",
            code_path=f"{self.Paths.LOCAL_ARTIFACTS_LAMBDA_CODE}/bot/",
            runtime=_lambda.Runtime.PYTHON_3_11,
            timeout=Duration.seconds(30),
            environment={
                "ENVIRONMENT": self.PROJECT_CONFIG.environment.value,
                "DYNAMODB_LOTE_TABLE": self.dynamodb_table_lote.table_name,
                "DYNAMODB_METADATA_TABLE": self.dynamodb_table_metadata.table_name,
                "DYNAMODB_STRUCTURE_TABLE": self.dynamodb_table_structure.table_name,
                "SECRET_NAME": self.secret_bot.secret_name
            },
            layers=[lambda_layer_powertools],
        )
        self.lambda_bot = self.builder.build_lambda_function(lambda_config)
        self.dynamodb_table_lote.grant_read_write_data(self.lambda_bot)
        self.dynamodb_table_metadata.grant_read_write_data(self.lambda_bot)
        self.dynamodb_table_structure.grant_read_write_data(self.lambda_bot)
        self.secret_bot.grant_read(self.lambda_bot)