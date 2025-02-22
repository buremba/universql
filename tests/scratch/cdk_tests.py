import boto3
import docker
import base64
import json


# Initialize boto3 ECR client
ecr = boto3.client('ecr')
repository_uri = event['ResourceProperties']['RepositoryUri']
public_image_uri = event['ResourceProperties']['PublicImageUri']
region = context.invoked_function_arn.split(':')[3]
registry = repository_uri.split('/')[0]

# Get ECR authorization token
auth_response = ecr.get_authorization_token()
token = auth_response['authorizationData'][0]['authorizationToken']
username, password = base64.b64decode(token).decode().split(':')

# Initialize Docker client
docker_client = docker.from_env()

# Login to ECR
docker_client.login(
    username=username,
    password=password,
    registry=registry
)

# Pull the public image
print(f"Pulling image: {public_image_uri}")
docker_client.images.pull(public_image_uri)

# Tag the image for our repository
source_image = docker_client.images.get(public_image_uri)
source_image.tag(repository_uri, tag='latest')

# Push to private repository
print(f"Pushing image to: {repository_uri}")
for line in docker_client.images.push(repository_uri, tag='latest', stream=True, decode=True):
    print(json.dumps(line))

# Get the image digest
describe_images = ecr.describe_images(
    repositoryName=event['ResourceProperties']['RepositoryName'],
    imageIds=[{'imageTag': 'latest'}]
)
image_digest = describe_images['imageDetails'][0]['imageDigest']
full_uri = f"{repository_uri}@{image_digest}"