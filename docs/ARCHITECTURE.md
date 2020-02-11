# Architecture

The following diagram provides an high-level overview of the system.

![Architecture](images/architecture.png)

## Requirements

After the deployment, the users are required to configure Cognito in order to add users to the UI or the API. After accessing the UI or the REST API, the users can configure the system by mapping S3 buckets to the solution.

## User Interface

For interacting with the system, the users can use the Web UI, or the API.

The Web UI allows authenticated requests to the API layer by using Amazon Cognito User Pools. It consists of a Amazon S3 static site.
The API Gateway allows to be used directly by users by sending authenticated requests.

## Deletion Job Workflow

The Deletion Job workflow is operated by a AWS Step Function that uses AWS Lambda for computing, Amazon DynamoDB and Amazon SQS to handle state, and nested AWS Step Functions to execute the Find and Forget phases.

![Architecture](images/stepfunctions_graph_main.png)

## The Find Workflow

The Find workflow is operated by a AWS Step Function that uses AWS Lambda for computing and Amazon Athena to query Amazon S3.

![Architecture](images/stepfunctions_graph_athena.png)

## The Forget Workflow

The Forget workflow is operated by a Amazon Step Function that uses AWS Lambda and AW Fargate for computing and Amazon DynamoDB and Amazon SQS to handle state.

![Architecture](images/stepfunctions_graph_deletion.png)
