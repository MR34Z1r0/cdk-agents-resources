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
    aws_apigateway as apigw,
    CfnOutput
)
from constructs import Construct
from aje_cdk_libs.builders.resource_builder import ResourceBuilder
from aje_cdk_libs.models.configs import *
from aje_cdk_libs.constants.environments import Environments
from aje_cdk_libs.constants.project_config import ProjectConfig

class CdkAgentsResourcesStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # Load configuration
        self.app_config = self.node.try_get_context("project_config")
        if not self.app_config:
            raise ValueError("Missing 'project_config' in context")
        
        # Create ProjectConfig instance
        self.PROJECT_CONFIG = ProjectConfig.from_dict(self.app_config)
        
        # Initialize resource builder
        self.builder = ResourceBuilder(self, self.PROJECT_CONFIG)
        
        # Setup paths
        self.LAMBDA_CODE_PATH = "artifacts/aws-lambda/code"
        
        # Import secrets
        self.secret_bot = self.builder.import_secret(
            f"{self.PROJECT_CONFIG.environment.value}/{self.PROJECT_CONFIG.project_name}/{self.PROJECT_CONFIG.app_config['secret_name']}"
        )
        
        # Create all resources
        self.create_dynamodb_tables()
        self.create_s3_buckets()
        self.create_lambda_layers()
        self.create_lambda_functions()
        self.create_api_gateway()
        self.create_outputs()
    
    def create_dynamodb_tables(self):
        """Create required DynamoDB tables"""
        # Chat History Table
        dynamodb_config = DynamoDBConfig(
            table_name="chat_history",
            partition_key="ALUMNO_ID",
            partition_key_type=dynamodb.AttributeType.STRING,
            sort_key="DATE_TIME",
            sort_key_type=dynamodb.AttributeType.STRING,
            removal_policy=RemovalPolicy.DESTROY
        )
        self.chat_history_table = self.builder.build_dynamodb_table(dynamodb_config)
        
        # Learning Resources Table
        dynamodb_config = DynamoDBConfig(
            table_name="learning_resources",
            partition_key="resource_id",
            partition_key_type=dynamodb.AttributeType.STRING,
            removal_policy=RemovalPolicy.DESTROY
        )
        self.learning_resources_table = self.builder.build_dynamodb_table(dynamodb_config)
        
        # Learning Resources Hash Table
        dynamodb_config = DynamoDBConfig(
            table_name="learning_resources_hash",
            partition_key="file_hash",
            partition_key_type=dynamodb.AttributeType.STRING,
            removal_policy=RemovalPolicy.DESTROY
        )
        self.learning_resources_hash_table = self.builder.build_dynamodb_table(dynamodb_config)
        
        # Library Table
        dynamodb_config = DynamoDBConfig(
            table_name="library",
            partition_key="silabus_id",
            partition_key_type=dynamodb.AttributeType.STRING,
            removal_policy=RemovalPolicy.DESTROY
        )
        self.library_table = self.builder.build_dynamodb_table(dynamodb_config)
    
    def create_s3_buckets(self):
        """Create S3 buckets for resource storage"""
        s3_config = S3Config(
            bucket_name="resources",
            versioned=False,
            removal_policy=RemovalPolicy.DESTROY,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL
        )
        self.resources_bucket = self.builder.build_s3_bucket(s3_config)
    
    def create_lambda_layers(self):
        """Create or reference required Lambda layers"""
        self.lambda_layer_powertools = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "LambdaPowertoolsLayer",
            layer_version_arn="arn:aws:lambda:us-east-1:017000801446:layer:AWSLambdaPowertoolsPythonV2:20"
        )
        self.lambda_layer_aje_libs = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "LambdaPowertoolsLayer",
            layer_version_arn="arn:aws:lambda:us-east-1:509399624591:layer:layer_aje_libs:1"
        )
        
        
    
    def create_lambda_functions(self):
        """Create all Lambda functions needed for the chatbot"""
        
        # Common environment variables for all Lambda functions
        common_env_vars = {
            "ENVIRONMENT": self.PROJECT_CONFIG.environment.value,
            "DYNAMO_CHAT_HISTORY_TABLE": self.chat_history_table.table_name,
            "DYNAMO_LIBRARY_TABLE": self.library_table.table_name,
            "DYNAMO_RESOURCES_TABLE": self.learning_resources_table.table_name,
            "DYNAMO_RESOURCES_HASH_TABLE": self.learning_resources_hash_table.table_name,
            "S3_BUCKET": self.resources_bucket.bucket_name,
            "PINECONE_API_KEY": "{{resolve:secretsmanager:" + self.secret_bot.secret_name + ":SecretString:PINECONE_API_KEY}}",
            "PINECONE_INDEX_NAME": "{{resolve:secretsmanager:" + self.secret_bot.secret_name + ":SecretString:PINECONE_INDEX_NAME}}",
            "EMBEDDINGS_MODEL_ID": "amazon.titan-embed-text-v2:0",
            "BEDROCK_CHATBOT_MODEL_ID": "us.meta.llama3-2-3b-instruct-v1:0",
            "BEDROCK_CHATBOT_REGION": "us-west-2",
            "BEDROCK_CHATBOT_LLM_MAX_TOKENS": "512",
            "HISTORY_CANT_ELEMENTS": "5",
            "PINECONE_MAX_RETRIEVE_DOCUMENTS": "5",
            "PINECONE_MIN_THRESHOLD": "0.75",
            "OWNER": self.PROJECT_CONFIG.author,
            "PROJECT_NAME": self.PROJECT_CONFIG.project_name
        }
        
        # Create ask Lambda function
        lambda_config = LambdaConfig(
            function_name="ask",
            handler="lambda_function.lambda_handler",
            code_path=f"{self.LAMBDA_CODE_PATH}/ask",
            runtime=_lambda.Runtime.PYTHON_3_11,
            memory_size=1024,
            timeout=Duration.seconds(60),
            environment=common_env_vars,
            layers=[self.lambda_layer_powertools, self.lambda_layer_aje_libs]
        )
        self.ask_lambda = self.builder.build_lambda_function(lambda_config)
        
        # Create delete_history Lambda function
        lambda_config = LambdaConfig(
            function_name="delete_history",
            handler="lambda_function.lambda_handler",
            code_path=f"{self.LAMBDA_CODE_PATH}/delete_history",
            runtime=_lambda.Runtime.PYTHON_3_11,
            memory_size=512,
            timeout=Duration.seconds(30),
            environment=common_env_vars,
            layers=[self.lambda_layer_powertools, self.lambda_layer_aje_libs]
        )
        self.delete_history_lambda = self.builder.build_lambda_function(lambda_config)
        
        # Create get_history Lambda function
        lambda_config = LambdaConfig(
            function_name="get_history",
            handler="lambda_function.lambda_handler",
            code_path=f"{self.LAMBDA_CODE_PATH}/get_history",
            runtime=_lambda.Runtime.PYTHON_3_11,
            memory_size=512,
            timeout=Duration.seconds(30),
            environment=common_env_vars,
            layers=[self.lambda_layer_powertools, self.lambda_layer_aje_libs]
        )
        self.get_history_lambda = self.builder.build_lambda_function(lambda_config)
        
        # Create add_resource Lambda function
        lambda_config = LambdaConfig(
            function_name="add_resource",
            handler="lambda_function.lambda_handler",
            code_path=f"{self.LAMBDA_CODE_PATH}/add_resource",
            runtime=_lambda.Runtime.PYTHON_3_11,
            memory_size=1024,
            timeout=Duration.seconds(60),
            environment=common_env_vars,
            layers=[self.lambda_layer_powertools, self.lambda_layer_aje_libs]
        )
        self.add_resource_lambda = self.builder.build_lambda_function(lambda_config)
        
        # Create delete_resource Lambda function
        lambda_config = LambdaConfig(
            function_name="delete_resource",
            handler="lambda_function.lambda_handler",
            code_path=f"{self.LAMBDA_CODE_PATH}/delete_resource",
            runtime=_lambda.Runtime.PYTHON_3_11,
            memory_size=512,
            timeout=Duration.seconds(30),
            environment=common_env_vars,
            layers=[self.lambda_layer_powertools, self.lambda_layer_aje_libs]
        )
        self.delete_resource_lambda = self.builder.build_lambda_function(lambda_config)
        
        # Grant permissions
        self.resources_bucket.grant_read_write(self.add_resource_lambda)
        self.resources_bucket.grant_read_write(self.delete_resource_lambda)
        
        self.chat_history_table.grant_read_write_data(self.ask_lambda)
        self.chat_history_table.grant_read_write_data(self.delete_history_lambda)
        self.chat_history_table.grant_read_write_data(self.get_history_lambda)
        
        self.library_table.grant_read_write_data(self.ask_lambda)
        self.library_table.grant_read_data(self.add_resource_lambda)
        self.library_table.grant_read_write_data(self.delete_resource_lambda)
        
        self.learning_resources_table.grant_read_write_data(self.add_resource_lambda)
        self.learning_resources_table.grant_read_write_data(self.delete_resource_lambda)
        self.learning_resources_hash_table.grant_read_write_data(self.add_resource_lambda)
        self.learning_resources_hash_table.grant_read_write_data(self.delete_resource_lambda)
        
        # Grant Bedrock permissions to Lambda functions
        bedrock_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "bedrock:InvokeModel",
                "bedrock:InvokeModelWithResponseStream",
                "bedrock:Converse"
            ],
            resources=["*"]
        )
        
        self.ask_lambda.add_to_role_policy(bedrock_policy)
        self.add_resource_lambda.add_to_role_policy(bedrock_policy)
    
    def create_api_gateway(self):
        """Create API Gateway for exposing Lambda functions"""
        
        # Create REST API
        api_gateway_config = ApiGatewayConfig(
            name="chatbot-api",
            description="Chatbot API Gateway",
            endpoint_types=[apigw.EndpointType.REGIONAL],
        )
        self.api = self.builder.build_api_gateway(api_gateway_config)
        
        # CORS configuration for all resources
        cors_options = apigw.CorsOptions(
            allow_origins=["*"],
            allow_methods=["GET", "POST", "OPTIONS"],
            allow_headers=["Content-Type", "Authorization"]
        )
        
        # Create API resources and methods
        ask_resource = self.api.root.add_resource("ask", default_cors_preflight_options=cors_options)
        ask_resource.add_method("POST", apigw.LambdaIntegration(self.ask_lambda))
        
        history_resource = self.api.root.add_resource("history", default_cors_preflight_options=cors_options)
        history_resource.add_method("POST", apigw.LambdaIntegration(self.get_history_lambda))
        
        delete_history_resource = self.api.root.add_resource("delete-history", default_cors_preflight_options=cors_options)
        delete_history_resource.add_method("POST", apigw.LambdaIntegration(self.delete_history_lambda))
        
        resources_resource = self.api.root.add_resource("resources", default_cors_preflight_options=cors_options)
        add_resource = resources_resource.add_resource("add", default_cors_preflight_options=cors_options)
        add_resource.add_method("POST", apigw.LambdaIntegration(self.add_resource_lambda))
        
        delete_resource = resources_resource.add_resource("delete", default_cors_preflight_options=cors_options)
        delete_resource.add_method("POST", apigw.LambdaIntegration(self.delete_resource_lambda))
        
        # Create deployment and stage
        deployment_config = ApiGatewayDeploymentConfig(
            deployment_name="prod-deployment",
            description="Production deployment",
            api=self.api
        )
        deployment = self.builder.build_api_gateway_deployment(deployment_config)
        
        stage_config = ApiGatewayStageConfig(
            stage_name="prod",
            deployment=deployment,
            logging_level="INFO",
            data_trace_enabled=True
        )
        self.prod_stage = self.builder.build_api_gateway_stage(stage_config)
    
    def create_outputs(self):
        """Create CloudFormation outputs for important resources"""
        CfnOutput(self, "ApiGatewayUrl", 
                 value=f"https://{self.api.rest_api_id}.execute-api.{self.region}.amazonaws.com/{self.prod_stage.stage_name}/",
                 description="API Gateway URL")
        
        CfnOutput(self, "ChatHistoryTableName", 
                 value=self.chat_history_table.table_name,
                 description="Chat History DynamoDB Table")
        
        CfnOutput(self, "LibraryTableName", 
                 value=self.library_table.table_name,
                 description="Library DynamoDB Table")
        
        CfnOutput(self, "ResourcesTableName", 
                 value=self.learning_resources_table.table_name,
                 description="Learning Resources DynamoDB Table")
        
        CfnOutput(self, "ResourcesBucketName", 
                 value=self.resources_bucket.bucket_name,
                 description="Resources S3 Bucket")