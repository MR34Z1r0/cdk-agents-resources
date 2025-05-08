# Built-in imports
from typing import Optional, Union
import uuid

# External imports
from aws_lambda_powertools import Logger

def custom_logger(
    name: Optional[str] = None,
    correlation_id: Optional[Union[str, uuid.UUID, None]] = None,
    service: Optional[str] = None,
    owner: Optional[str] = None,
) -> Logger:
    """Returns a custom <aws_lambda_powertools.Logger> Object."""
    return Logger(
        name=name,
        correlation_id=correlation_id,
        service=service,
        owner=owner,
        log_uncaught_exceptions=True,
    )
