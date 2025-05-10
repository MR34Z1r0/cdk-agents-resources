import os
from aje_cdk_libs.constants.project_config import ProjectConfig

class Paths:
    """Centralized path configurations for local and AWS assets"""
    def __init__(self, app_config: dict):
        self.LOCAL_ARTIFACTS = app_config.get("artifacts").get("local")
        
        # Local paths 
        self.LOCAL_ARTIFACTS_LAMBDA = f'{self.LOCAL_ARTIFACTS}/aws-lambda' 
        self.LOCAL_ARTIFACTS_LAMBDA_CODE = f'{self.LOCAL_ARTIFACTS_LAMBDA}/code' 
        self.LOCAL_ARTIFACTS_LAMBDA_LAYER = f'{self.LOCAL_ARTIFACTS_LAMBDA}/layer' 
        self.LOCAL_ARTIFACTS_LAMBDA_DOCKER = f'{self.LOCAL_ARTIFACTS_LAMBDA}/docker' 
        
        # AWS paths 