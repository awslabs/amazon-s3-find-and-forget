Cost Overview
=============

Amazon S3 Find and Forget is a solution you deploy in your own AWS account using [AWS CloudFormation]. There is no charge for the solution: you pay only for
the AWS services used to run the solution. This page outlines the services used by the solution, and examples of the charges you should expect for typical
usage of the solution.

> **Disclaimer**
>
> You are responsible for the cost of the AWS services used while running this deployment. There is no additional cost for using the solution. For full details, see the following pricing pages for each AWS service you will be using. Prices are subject to change.

# Index

* [Overview](#overview)
  * [Amazon VPC](#amazon-vpc)
  * [Amazon Athena](#amazon-athena)
  * [Amazon S3](#amazon-s3)
  * [AWS Fargate](#aws-fargate)
  * [Amazon DynamoDB](#amazon-dynamodb)
  * [Amazon SQS](#amazon-sqs)
  * [Amazon API Gateway](#amazon-api-gateway)
  * [AWS Lambda](#aws-lambda)
  * [AWS Step Functions](#aws-step-functions)
  * [AWS Glue](#aws-glue)
  * [Amazon Cognito](#amazon-cognito)
  * [Amazon CloudFront](#amazon-cloudfront)
  * [Other Supporting Services](#other-supporting-services)
* [Solution Cost Estimate](#solution-cost-estimate)
  * [Scenario 1](#scenario-1)

# Overview

Where possible, the solution only uses Serverless components. All the components for Web UI, API and Deletion Jobs are Serverless.
The services that most significantly affect cost in the Amazon S3 Find and Forget solution are Amazon Athena (during the Find phase), Amazon S3 (for reading and writing during the Forget Phase) and AWS Fargate (during the Forget Phase)

## Amazon VPC

The solution [needs a VPC to run][VPC Configuration].  By itself a VPC does not incur costs however if the VPC you choose to use with the solution contains components such as VPC Endpoints or NAT Gateways, these may incur additional costs. The sample VPC provided as part of this solution does not make use of NAT gateways, however it does provision VPC endpoints which will incur charges. See [Amazon VPC Pricing] for more information.

* [Amazon VPC Pricing]
* [AWS PrivateLink Pricing]

## Amazon Athena

Amazon Athena is the service currently used for the Find phase. You are charged based on the amount of data scanned by each query. You can get significant cost savings and performance gains by compressing, partitioning, or converting your data to a columnar format, because each of those operations reduces the amount of data that Athena needs to scan to execute a query.

The [Amazon Athena Pricing] page contains a comprehensive overview of the costs and includes a calculator that can be used for estimating the cost of each Job run based on the Data Lake size.

## Amazon S3

There are 4 components affecting cost when working with Amazon S3: Storage, Requests and data retrievals, Data Transfer, and Management.

During the Forget phase, the cost is affected by the number of objects to process and their size. For each object, a task will read the entire content and metadata of each object from S3, it will write it to a staging bucket, it will then delete the original object, and move the updated object to source bucket together with its metadata.

Additional costs apply as Amazon S3 is also used by the solution to handle internal state during Step Function executions and during the solution deployment.

[Amazon S3 Pricing]

## AWS Fargate

AWS Fargate is used by the solution during the Forget Phase. The cost is affected by the number of containers and their configuration (vCPU and memory), configurable when deploying the Solution.

[AWS Fargate Pricing]

## Amazon DynamoDB

Amazon DynamoDB is used for storing internal state. All the tables are setup to use on-demand capacity mode after the solution is deployed.

* [Amazon DynamoDB Pricing]
* [Solution Persistence Layer]

## Amazon SQS

The system uses a number of queues (some Standard, some FIFO) to handle internal state. The number of partitions of specific AWS Glue Data Catalogs and the number of Amazon S3 Objects to process impact the size and the number of SQS messages that the system will processes during a Deletion Job, which will affect the overall cost.

[Amazon SQS Pricing]

## Amazon API Gateway

Amazon API Gateway is serverless, so you only pay when your APIs are in use. There are no minimum fees or upfront commitments and you pay only for the API calls you receive and the amount of data transferred out.

[Amazon API Gateway Pricing]

## AWS Lambda

The solution uses AWS Lambda for API handlers, Step Function steps, and DynamoDB streams. With AWS Lambda, you pay only for what you use. You are charged based on the number of requests for your functions and the duration, the time it takes for your code to execute.

[AWS Lambda Pricing]

## AWS Step Functions

Step Functions state machines are used by the solution when a deletion job runs. You are charged based on the number of state transitions of each Step Function. Step Functions counts a state transition each time a step of your workflow is executed. You are charged for the total number of state transitions across all your state machines, including retries.

[AWS Step Functions Pricing]
[Deletion Job Workflow]

## AWS Glue

The solution uses the AWS Glue Data Catalog to fetch metadata about the Data Lake during the Find phase. For the AWS Glue Data Catalog you pay a simple monthly fee for storing and accessing the metadata and an additional fee based on the number of requests.

[AWS Glue Pricing]

## Amazon Cognito

With Amazon Cognito, you pay only for what you use. The solution uses Amazon Cognito to secure the API and an admin user is created during deployment.

[Amazon Cognito Pricing]

## Amazon CloudFront

CloudFront can be optionally included to distribute the Web UI when deploying the solution.

[Amazon CloudFront Pricing]

## Other Supporting Services

During the deployment the solution uses [AWS CodeBuild], [AWS CodePipeline] and [AWS Lambda] custom resources to deploy the front-end and the back-end.
[AWS Fargate] uses [Amazon Elastic Container Registry] to store container images.

# Solution Cost Estimate

You are responsible for the cost of the AWS services used while running this solution. As of the date of publication, the estimated cost to run a job with different Data Lake configurations in the `eu-west-1` region is shown in the tables below. The estimates do not include VPC costs.

## Scenario 1

Deletion job for 100GB of Snappy compressed Parquet objects with 2 Glue Partitions (scanned: 6.8GB - processed: 100GB)
|Service|Spending|
|-|-|
|Amazon Athena|0.03$|
|AWS Fargate|0.04$|
|Amazon S3|0.01$|
|Other services|0.01$|
|Total|0.09$|

[VPC Configuration]: USER_GUIDE.md#pre-requisite-Configuring-a-vpc-for-the-solution
[some VPC endpoints]: [https://github.com/awslabs/amazon-s3-find-and-forget/blob/master/templates/vpc.yaml]
[Amazon API Gateway Pricing]: https://aws.amazon.com/api-gateway/pricing/
[Amazon Athena Pricing]: https://aws.amazon.com/athena/pricing/
[Amazon CloudFront Pricing]: https://aws.amazon.com/cloudfront/pricing/
[Amazon Cognito Pricing]: https://aws.amazon.com/cognito/pricing/
[Amazon DynamoDB Pricing]: https://aws.amazon.com/dynamodb/pricing/
[Amazon Elastic Container Registry]: https://aws.amazon.com/ecr/pricing/
[Amazon S3 Pricing]: https://aws.amazon.com/s3/pricing/
[Amazon SQS Pricing]: https://aws.amazon.com/sqs/pricing/
[Amazon VPC Pricing]: https://aws.amazon.com/vpc/pricing/
[AWS CloudFormation]: https://aws.amazon.com/cloudformation/
[AWS CodeBuild]: https://aws.amazon.com/codebuild/pricing/
[AWS CodePipeline]: https://aws.amazon.com/codepipeline/pricing/
[AWS Fargate]: https://aws.amazon.com/fargate/pricing/
[AWS Fargate Pricing]: https://aws.amazon.com/fargate/pricing/
[AWS Glue Pricing]: https://aws.amazon.com/glue/pricing/
[AWS Lambda Pricing]: https://aws.amazon.com/lambda/pricing/
[AWS Lambda]: https://aws.amazon.com/lambda/pricing/
[AWS PrivateLink Pricing]: https://aws.amazon.com/privatelink/pricing/
[AWS Step Functions Pricing]: https://aws.amazon.com/step-functions/pricing/
[Deletion Job Workflow]: ARCHITECTURE.md#deletion-job-workflow
[Solution Persistence Layer]: ARCHITECTURE.md#persistence-layer
