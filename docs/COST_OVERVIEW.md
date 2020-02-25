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

The solution uses Serverless components. All the components for Web UI, API and Deletion Jobs are Serverless.

The services that most significantly affect cost in the Amazon S3 Find and Forget solution are Amazon Athena (during the Find phase), Amazon S3 (for reading and writing during the Forget Phase) and AWS Fargate (during the Forget Phase)

## Amazon VPC

Amazon VPC provides network connectivity for AWS Fargate tasks that run during the _Forget_ phase. 

How you build the VPC used can determine the prices you pay. For example, VPC Endpoints and NAT Gateways have different hourly prices and costs for data transferred.

The sample VPC provided in this solution makes use of VPC Endpoints, which have an hourly cost as well as data transfer cost. You can choose to use this sample VPC,
however it may be more cost-efficient to use an existing suitable VPC in your account if one exists.

* [Amazon VPC Pricing]
* [AWS PrivateLink Pricing]

## Amazon Athena

Amazon Athena scans your data lake during the _Find phase_ of a deletion job. You pay for the Athena queries run based on the amount of data scanned.

You can achieve significant cost savings and performance gains by reducing the quantity of data Athena needs to scan per query by using compression, partitioning and conversion of your data to a columnar format. See [Supported Data Formats](LIMITS.md#supported-data-formats) for more information regarding supported data and compression formats.

The [Amazon Athena Pricing] page contains an overview of prices and provides a calculator to estimate the Athena query cost for each deletion job run based on the Data
Lake size.

## Amazon S3

Four types of charges occur when working with Amazon S3: Storage, Requests and data retrievals, Data Transfer, and Management.

- The solution web interface is deployed to, and served, from an S3 Bucket
- During the _Find_ phase, Amazon Athena will:
  1. Retrieve data from Amazon S3 for the columns defined in the data mapper
  1. Store its results in an S3 bucket
- During the _Forget_ phase, a program run in AWS Fargate processes each object identified in the Find phase will:
  1. Retrieve the entire object and its metadata
  1. Create a new version of the file, and PUTs this object to a staging bucket
  1. Deletes the original object
  1. Copies the updated object from the staging bucket to the data bucket, and sets any metadata identified from the original object
  1. Deletes the object from the staging bucket
- Some small artefacts, and state data relating to AWS Step Functions Workflows may be stored in S3

[Amazon S3 Pricing]

## AWS Fargate

The Forget phase of the solution uses AWS Fargate. Using Fargate, you pay for the duration that Fargate tasks run during the Forget phase.

The AWS Fargate cost is affected by the number of Fargate tasks you choose to run concurrently, and their configuration (vCPU and memory). You can configure these
parameters when deploying the Solution.

[AWS Fargate Pricing]

## Amazon DynamoDB

Amazon DynamoDB stores internal state data for the solution. All tables created by the solution use the on-demand capacity mode of pricing. You pay for storage used by
these tables, and capacity used when interacting with the solution web interface, API, or running a deletion job.

* [Amazon DynamoDB Pricing]
* [Solution Persistence Layer]

## Amazon SQS

The solution uses standard and FIFO SQS queues to handle internal state during a deletion job. You pay for the number of requests made to SQS. The number of requests
increases with the number of data mappers, partitions in those data mappers, and the number of Amazon S3 objects processed in a deletion job.

[Amazon SQS Pricing]

## Amazon API Gateway

Amazon API Gateway is used to provide the solution web interface and API. You pay for requests made when using the web interface or API, and any data transferred out.

[Amazon API Gateway Pricing]

## AWS Lambda

AWS Lambda Functions are used throughout the solution. You pay for the requests to, and execution time of these functions. Functions execute when using the solution web
interface, API, and when a deletion job runs.

[AWS Lambda Pricing]

## AWS Step Functions

AWS Step Functions Standard Workflows are used when a deletion job runs. You pay for the amount of state transitions in the Step Function Workflow. The number of
state transitions will increase with the number of data mappers, and partitions in those data mappers, included in a deletion job.

[AWS Step Functions Pricing]
[Deletion Job Workflow]

## AWS Glue

AWS Glue Data Catalog is used by the solution to define data mappers. You pay a monthly fee based on the number of objects stored in the data catalog, and for requests
made to the AWS Glue service when the solution runs.

[AWS Glue Pricing]

## Amazon Cognito

Amazon Cognito provides authentication to secure access to the API using an administrative user created during deployment. You pay a monthly fee for active users in
the Cognito User Pool.

[Amazon Cognito Pricing]

## Amazon CloudFront

If you choose to deploy a CloudFront distribution for the solution interface, you will pay CloudFront charges for requests and data transferred when you access the web
interface.

[Amazon CloudFront Pricing]

## Other Supporting Services

During deployment, the solution uses [AWS CodeBuild], [AWS CodePipeline] and [AWS Lambda] custom resources to deploy the frontend and the backend.
[AWS Fargate] uses [Amazon Elastic Container Registry] to store container images.

# Solution Cost Estimate

You are responsible for the cost of the AWS services used while running this solution. As of the date of publication of this version of the source code, the estimated cost to run a job with different Data Lake configurations in the `eu-west-1` region is shown in the tables below. The estimates do not include VPC costs.

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
