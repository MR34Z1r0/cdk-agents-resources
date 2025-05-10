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
from constants.paths import Paths
from constants.layers import Layers
import os
from dotenv import load_dotenv
import urllib.parse
from aje_cdk_libs.constants.project_config import ProjectConfig

class CdkAgentsResourcesStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, project_config: ProjectConfig, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)         
        self.PROJECT_CONFIG = project_config        
        self.builder = ResourceBuilder(self, self.PROJECT_CONFIG)
        self.Paths = Paths(self.PROJECT_CONFIG.app_config)
        self.Layers = Layers(self.PROJECT_CONFIG.app_config, project_config.region_name, project_config.account_id)
 
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
            layer_version_arn=self.Layers.AWS_LAMBDA_LAYERS.get("layer_powertools")
        )
        
        self.lambda_layer_aje_libs = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "LambdaAjeLibsLayer",
            layer_version_arn=self.Layers.AWS_LAMBDA_LAYERS.get("layer_aje_libs")
        )
        
        self.lambda_layer_pinecone = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "LambdaPineconeLayer",
            layer_version_arn=self.Layers.AWS_LAMBDA_LAYERS.get("layer_pinecone")
        )
        
        self.lambda_layer_docs = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "LambdaDocsLayer",
            layer_version_arn=self.Layers.AWS_LAMBDA_LAYERS.get("layer_docs")
        )
        
        self.lambda_layer_requests = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "LambdaRequestsLayer",
            layer_version_arn=self.Layers.AWS_LAMBDA_LAYERS.get("layer_requests")
        )
    
    def create_lambda_functions(self):
        """Create all Lambda functions needed for the chatbot"""
        
        # Common environment variables for all Lambda functions
        common_env_vars = {
            "ENVIRONMENT": self.PROJECT_CONFIG.environment.value.lower(),
            "PROJECT_NAME": self.PROJECT_CONFIG.project_name,
            "OWNER": self.PROJECT_CONFIG.author,
            "DYNAMO_CHAT_HISTORY_TABLE": self.chat_history_table.table_name,
            "DYNAMO_LIBRARY_TABLE": self.library_table.table_name,
            "DYNAMO_RESOURCES_TABLE": self.learning_resources_table.table_name,
            "DYNAMO_RESOURCES_HASH_TABLE": self.learning_resources_hash_table.table_name,
            "S3_RESOURCES_BUCKET": self.resources_bucket.bucket_name
        }
        
        # Create ask Lambda function
        function_name = "ask"
        lambda_config = LambdaConfig(
            function_name=function_name,
            handler=f"{function_name}/lambda_function.lambda_handler",
            code_path=f"{self.Paths.LOCAL_ARTIFACTS_LAMBDA_CODE}/chatbot",
            runtime=_lambda.Runtime.PYTHON_3_11,
            memory_size=1024,
            timeout=Duration.seconds(60),
            environment=common_env_vars,
            layers=[self.lambda_layer_powertools, self.lambda_layer_aje_libs, self.lambda_layer_pinecone]
        )
        self.ask_lambda = self.builder.build_lambda_function(lambda_config)
        
        # Create delete_history Lambda function
        function_name = "delete_history"
        lambda_config = LambdaConfig(
            function_name=function_name,
            handler=f"{function_name}/lambda_function.lambda_handler",
            code_path=f"{self.Paths.LOCAL_ARTIFACTS_LAMBDA_CODE}/chatbot",
            runtime=_lambda.Runtime.PYTHON_3_11,
            memory_size=512,
            timeout=Duration.seconds(30),
            environment=common_env_vars,
            layers=[self.lambda_layer_powertools, self.lambda_layer_aje_libs]
        )
        self.delete_history_lambda = self.builder.build_lambda_function(lambda_config)
        
        # Create get_history Lambda function
        function_name = "get_history"
        lambda_config = LambdaConfig(
            function_name=function_name,
            handler=f"{function_name}/lambda_function.lambda_handler",
            code_path=f"{self.Paths.LOCAL_ARTIFACTS_LAMBDA_CODE}/chatbot",
            runtime=_lambda.Runtime.PYTHON_3_11,
            memory_size=512,
            timeout=Duration.seconds(30),
            environment=common_env_vars,
            layers=[self.lambda_layer_powertools, self.lambda_layer_aje_libs]
        )
        self.get_history_lambda = self.builder.build_lambda_function(lambda_config)
        
        # Create add_resource Lambda Docker function 
        function_name = "add_resource"       
        docker_image = _lambda.DockerImageCode.from_image_asset(
            directory=f"{self.Paths.LOCAL_ARTIFACTS_LAMBDA_DOCKER}/chatbot/{function_name}",
        )
        
        lambda_config = LambdaDockerConfig(
            function_name=function_name,
            code=docker_image,
            memory_size=1024,
            timeout=Duration.seconds(60),
            environment=common_env_vars
        )
        self.add_resource_lambda = self.builder.build_lambda_docker_function(lambda_config)
        
        # Create delete_resource Lambda function
        function_name = "delete_resource"
        lambda_config = LambdaConfig(
            function_name=function_name,
            handler=f"{function_name}/lambda_function.lambda_handler",
            code_path=f"{self.Paths.LOCAL_ARTIFACTS_LAMBDA_CODE}/chatbot",
            runtime=_lambda.Runtime.PYTHON_3_11,
            memory_size=512,
            timeout=Duration.seconds(30),
            environment=common_env_vars,
            layers=[self.lambda_layer_powertools, self.lambda_layer_aje_libs, self.lambda_layer_pinecone]
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
        
        # Grant Bedrock permissions to Lambda functions
        ssm_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "ssm:GetParameter",
                "ssm:GetParameters"
            ],
            resources=["*"]
        )
        
        secrets_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "secretsmanager:GetSecretValue"                
            ],
            resources=["*"]
        )
        
        self.ask_lambda.add_to_role_policy(bedrock_policy)
        self.add_resource_lambda.add_to_role_policy(bedrock_policy)
        
        self.ask_lambda.add_to_role_policy(ssm_policy)
        self.add_resource_lambda.add_to_role_policy(ssm_policy)
        self.delete_resource_lambda.add_to_role_policy(ssm_policy)
        self.get_history_lambda.add_to_role_policy(ssm_policy)
        self.delete_history_lambda.add_to_role_policy(ssm_policy) 

        self.ask_lambda.add_to_role_policy(secrets_policy)
        self.add_resource_lambda.add_to_role_policy(secrets_policy)
        self.delete_resource_lambda.add_to_role_policy(secrets_policy)
        self.get_history_lambda.add_to_role_policy(secrets_policy)
        self.delete_history_lambda.add_to_role_policy(secrets_policy) 
        
    def create_api_gateway(self):
        """
        Method to create the REST-API Gateway for exposing the chatbot
        functionalities.
        """ 
        # Create the API Gateway without specifying a default handler
        self.api = apigw.RestApi(
            self,
            f"{self.PROJECT_CONFIG.app_config['api_gw_name']}-{self.PROJECT_CONFIG.environment.value.lower()}",
            description=f"REST API Gateway for {self.PROJECT_CONFIG.project_name} in {self.PROJECT_CONFIG.environment.value} environment",
            deploy_options=apigw.StageOptions(
                stage_name=self.PROJECT_CONFIG.environment.value.lower(),
                description=f"REST API for {self.PROJECT_CONFIG.project_name}",
                metrics_enabled=True,
            ),    
            default_method_options=apigw.MethodOptions(
                api_key_required=False,
                authorization_type=apigw.AuthorizationType.NONE,
            ),
            endpoint_types=[apigw.EndpointType.REGIONAL],
            cloud_watch_role=False,
        )
        
        # Define REST-API resources
        root_resource_api = self.api.root.add_resource("api")
        root_resource_v1 = root_resource_api.add_resource("v1")

        # Endpoints for the main functionalities
        root_resource_ask = root_resource_v1.add_resource("ask") 
        root_resource_delete_history = root_resource_v1.add_resource("delete_history")
        root_resource_get_history = root_resource_v1.add_resource("get_history")
        root_resource_add_resource = root_resource_v1.add_resource("add_resource")
        root_resource_delete_resource = root_resource_v1.add_resource("delete_resource")

        # Define all API-Lambda integrations for the API methods
        root_resource_ask.add_method("POST", apigw.LambdaIntegration(self.ask_lambda))
        root_resource_delete_history.add_method("POST", apigw.LambdaIntegration(self.delete_history_lambda))
        root_resource_get_history.add_method("POST", apigw.LambdaIntegration(self.get_history_lambda))
        root_resource_add_resource.add_method("POST", apigw.LambdaIntegration(self.add_resource_lambda))
        root_resource_delete_resource.add_method("POST", apigw.LambdaIntegration(self.delete_resource_lambda))
        
        # Store the deployment stage for use in outputs
        self.deployment_stage = self.PROJECT_CONFIG.environment.value.lower()
        
    def create_outputs(self):
        """Create CloudFormation outputs for important resources"""
        
        CfnOutput(self, "ResourcesBucketName", 
                value=self.resources_bucket.bucket_name,
                description="Resources S3 Bucket")
        
        CfnOutput(self, "ApiGatewayUrl", 
                value=f"https://{self.api.rest_api_id}.execute-api.{self.region}.amazonaws.com/{self.deployment_stage}/",
                description="API Gateway URL")
         