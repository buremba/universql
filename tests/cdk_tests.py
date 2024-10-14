import aws_cdk as cdk
from constructs import Construct
import aws_cdk.aws_lambda as _lambda


class HelloCdkStack(cdk.Stack):

  def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
    super().__init__(scope, construct_id, **kwargs)

    # Modify the Lambda function resource
    my_function = _lambda.Function(
      self, "HelloWorldFunction",
      runtime = _lambda.Runtime.NODEJS_20_X, # Provide any supported Node.js runtime
      handler = "index.handler",
      code = _lambda.Code.from_inline(
        """
        exports.handler = async function(event) {
          return {
            statusCode: 200,
            body: JSON.stringify('Hello CDK!'),
          };
        };
        """
      ),
    )
    my_function.add_function_url()

app = cdk.App()
HelloCdkStack(app, "TestStack")

synth = app.synth()
print(synth)
