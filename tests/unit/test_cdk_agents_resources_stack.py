import aws_cdk as core
import aws_cdk.assertions as assertions

from cdk_agents_resources.cdk_agents_resources_stack import CdkAgentsResourcesStack

# example tests. To run these tests, uncomment this file along with the example
# resource in cdk_agents_resources/cdk_agents_resources_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = CdkAgentsResourcesStack(app, "cdk-agents-resources")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
