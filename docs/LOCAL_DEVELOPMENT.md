# Local Development

This section details how to run the solution locally and deploy
your code changes from the command line. 

## Pre-Reqs

The following dependencies must be installed:
* AWS CLI
* Python >=3.7 and pip
* node.js >= v12
* virtualenv
* Ruby >= 2.6

Once you have installed all pre-requisites, you must run the following
command to create a `virtualenv` and install all frontend/backend
dependencies before commencing development.

```bash
make setup
```

This command only needs to be ran once.

## Build and Deploy from Source

To deploy the solution manually from source to your AWS account, run
the following command:

```bash
make deploy \
  REGION=<aws-region> \
  ADMIN_EMAIL=<your-email-address> \
  TEMP_BUCKET=<temp-bucket-name>
```

This will deploy the Amazon S3 Find and Forget solution using the AWS CLI
profile of the current shell. By default this will be the profile `default`.

The following commands are also available:

- `make deploy-artefacts`: Packages and uploads the Forget task Docker image
and frontend React app to the solution bucket. This will trigger CodePipeline
to automatically deploy these artefacts 
- `make deploy-cfn`: Deploys only the CloudFormation template
- `make deploy-containers-override`: Manually packages and deploys the
Forget task Docker image to ECR via the AWS CLI rather than using CodePipeline.
- `make deploy-frontend-override`: Manually packages and deploys the
frontend React app to S3 via the AWS CLI rather than using CodePipeline.
- `make start-frontend-remote`: Opens the frontend of the deployed Amazon S3
Find and Forget solution

## Running Locally

> **Important**: Running the frontend/forget task locally requires the
> solution CloudFormation stack to be deployed. For more info, see 
> [Build and Deploy From Source](#build-and-deploy-from-source)

To run the frontend locally, run the following commands:

- `make setup-frontend-local-dev`: Downloads a copy of the configuration file
required for the frontend app to run locally
- `make start-frontend-local`: Runs the frontend app locally on
`localhost:3000`

> In order to allow your locally running frontend to connect to the deployed
> API, you will need to set the `AccessControlAllowOriginOverride` parameter
> to * when deploying the solution stack

To run the "Forget" task locally using Docker, run the following command:
```bash
docker build -f backend/ecs_tasks/delete_files/Dockerfile -t s3f2 .
make run-local-container ROLE_NAME=<your-sqs-access-role-name>
```

The container needs to connect to the deletion queue deployed by the solution
and therefore AWS credentials are required in the container environment. You
will need to setup an IAM role which has access to process messages from the
queue and provide the role name as an input. The above command will perform STS
Assume Role via the AWS CLI using `ROLE_NAME` as the target role in order to
obtain temporary credentials. These temporary credentials will be injected into
the container as environment variables.

The command uses your default CLI profile to assume the role. You can override
the profile being used as follows:
```bash
make run-local-container ROLE_NAME=<your-sqs-access-role-name> AWS_PROFILE=my-profile
```

#### Run Tests

> **Important**: Running acceptance tests requires the solution CloudFormation
> stack to be deployed. For more info, see
> [Build and Deploy From Source](#build-and-deploy-from-source)

The following commands are available for running tests:

- `make test`: Run all unit and acceptance tests for the backend and frontend 
- `make test-acceptance`: Run all backend task acceptance tests
- `make test-unit`: Run all backend task unit tests
- `make test-frontend`: Run all frontend tests
