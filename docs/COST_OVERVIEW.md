# Cost Overview

Amazon S3 Find and Forget is a solution you deploy in your own AWS account using
[AWS CloudFormation]. There is no charge for the solution: you pay only for the
AWS services used to run the solution. This page outlines the services used by
the solution, and examples of the charges you should expect for typical usage of
the solution.

> **Disclaimer**
>
> You are responsible for the cost of the AWS services used while running this
> deployment. There is no additional cost for using the solution. For full
> details, see the following pricing pages for each AWS service you will be
> using. Prices are subject to change.

## Index

- [Overview](#overview)
  - [AWS Fargate](#aws-fargate)
  - [AWS Glue](#aws-glue)
  - [AWS Lambda](#aws-lambda)
  - [AWS Step Functions](#aws-step-functions)
  - [Amazon API Gateway](#amazon-api-gateway)
  - [Amazon Athena](#amazon-athena)
  - [Amazon CloudFront](#amazon-cloudfront)
  - [Amazon Cognito](#amazon-cognito)
  - [Amazon DynamoDB](#amazon-dynamodb)
  - [Amazon S3](#amazon-s3)
  - [Amazon SQS](#amazon-sqs)
  - [Amazon VPC](#amazon-vpc)
  - [Other Supporting Services](#other-supporting-services)
- [Solution Cost Estimate](#solution-cost-estimate)
  - [Scenario 1](#scenario-1)
  - [Scenario 2](#scenario-2)
  - [Scenario 3](#scenario-3)
  - [Scenario 4](#scenario-4)
  - [Scenario 5](#scenario-5)

## Overview

The Amazon S3 Find and Forget solution uses a serverless computing architecture.
This model minimises costs when you're not actively using the solution, and
allows the solution to scale while only paying for what you use.

The sample VPC provided in this solution makes use of VPC Endpoints, which have
an hourly cost as well as data transfer cost. All the other costs depend on the
usage of the API, and for typical usage, the greatest proportion of what you pay
will be for use of Amazon Athena, Amazon S3 and AWS Fargate.

### AWS Fargate

The Forget phase of the solution uses AWS Fargate. Using Fargate, you pay for
the duration that Fargate tasks run during the Forget phase.

The AWS Fargate cost is affected by the number of Fargate tasks you choose to
run concurrently, and their configuration (vCPU and memory). You can configure
these parameters when deploying the Solution.

[AWS Fargate Pricing]

### AWS Glue

AWS Glue Data Catalog is used by the solution to define data mappers. You pay a
monthly fee based on the number of objects stored in the data catalog, and for
requests made to the AWS Glue service when the solution runs.

[AWS Glue Pricing]

### AWS Lambda

AWS Lambda Functions are used throughout the solution. You pay for the requests
to, and execution time of, these functions. Functions execute when using the
solution web interface, API, and when a deletion job runs.

[AWS Lambda Pricing]

### AWS Step Functions

AWS Step Functions Standard Workflows are used when a deletion job runs. You pay
for the amount of state transitions in the Step Function Workflow. The number of
state transitions will increase with the number of data mappers, and partitions
in those data mappers, included in a deletion job.

[AWS Step Functions Pricing][deletion job workflow]

### Amazon API Gateway

Amazon API Gateway is used to provide the solution web interface and API. You
pay for requests made when using the web interface or API, and any data
transferred out.

[Amazon API Gateway Pricing]

### Amazon Athena

Amazon Athena scans your data lake during the _Find phase_ of a deletion job.
You pay for the Athena queries run based on the amount of data scanned.

You can achieve significant cost savings and performance gains by reducing the
quantity of data Athena needs to scan per query by using compression,
partitioning and conversion of your data to a columnar format. See
[Supported Data Formats](LIMITS.md#supported-data-formats) for more information
regarding supported data and compression formats.

The [Amazon Athena Pricing] page contains an overview of prices and provides a
calculator to estimate the Athena query cost for each deletion job run based on
the Data Lake size. See [Using Workgroups to Control Query Access and Costs] for
more information on using workgroups to set limits on the amount of data each
query or the entire workgroup can process, and to track costs.

### Amazon CloudFront

If you choose to deploy a CloudFront distribution for the solution interface,
you will pay CloudFront charges for requests and data transferred when you
access the web interface.

[Amazon CloudFront Pricing]

### Amazon Cognito

Amazon Cognito provides authentication to secure access to the API using an
administrative user created during deployment. You pay a monthly fee for active
users in the Cognito User Pool.

[Amazon Cognito Pricing]

### Amazon DynamoDB

Amazon DynamoDB stores internal state data for the solution. All tables created
by the solution use the on-demand capacity mode of pricing. You pay for storage
used by these tables, and DynamoDB capacity used when interacting with the
solution web interface, API, or running a deletion job.

- [Amazon DynamoDB Pricing]
- [Solution Persistence Layer]

### Amazon S3

Four types of charges occur when working with Amazon S3: Storage, Requests and
data retrievals, Data Transfer, and Management.

Uses of Amazon S3 in the solution include:

- The solution web interface is deployed to, and served, from an S3 Bucket
- During the _Find_ phase, Amazon Athena will:
  1. Retrieve data from Amazon S3 for the columns defined in the data mapper
  1. Store its results in an S3 bucket
- During the _Forget_ phase, a program run in AWS Fargate processes each object
  identified in the Find phase will:
  1. Retrieve the entire object and its metadata
  1. Create a new version of the file, and PUT this object to a staging bucket
  1. Delete the original object
  1. Copy the updated object from the staging bucket to the data bucket, and
     sets any metadata identified from the original object
  1. Delete the object from the staging bucket
- Some artefacts, and state data relating to AWS Step Functions Workflows may be
  stored in S3

[Amazon S3 Pricing]

### Amazon SQS

The solution uses standard and FIFO SQS queues to handle internal state during a
deletion job. You pay for the number of requests made to SQS. The number of
requests increases with the number of data mappers, partitions in those data
mappers, and the number of Amazon S3 objects processed in a deletion job.

[Amazon SQS Pricing]

### Amazon VPC

Amazon VPC provides network connectivity for AWS Fargate tasks that run during
the _Forget_ phase.

How you build the VPC will determine the prices you pay. For example, VPC
Endpoints and NAT Gateways are two different ways to provide network access to
the solutions' dependencies. Both ways have different hourly prices and costs
for data transferred.

The sample VPC provided in this solution makes use of VPC Endpoints, which have
an hourly cost as well as data transfer cost. You can choose to use this sample
VPC, however it may be more cost-efficient to use an existing suitable VPC in
your account if you have one.

- [Amazon VPC Pricing]
- [AWS PrivateLink Pricing]

### Other Supporting Services

During deployment, the solution uses [AWS CodeBuild], [AWS CodePipeline] and
[AWS Lambda] custom resources to deploy the frontend and the backend. [AWS
Fargate] uses [Amazon Elastic Container Registry] to store container images.

## Solution Cost Estimate

You are responsible for the cost of the AWS services used while running this
solution. As of the date of publication of this version of the source code, the
estimated cost to run a job with different Data Lake configurations in the
Europe (Ireland) region is shown in the tables below. The estimates do not
include VPC costs.

| Summary                   |                      |
| ------------------------- | -------------------- |
| [Scenario 1](#scenario-1) | 100GB Snappy Parquet |
| [Scenario 2](#scenario-2) | 750GB Snappy Parquet |
| [Scenario 3](#scenario-3) | 10TB Snappy Parquet  |
| [Scenario 4](#scenario-4) | 50TB Snappy Parquet  |
| [Scenario 5](#scenario-5) | 100GB Gzip JSON      |

### Scenario 1

This example shows how the charges would be calculated for a deletion job where:

- Your dataset is 100GB of Snappy compressed Parquet objects that are
  distributed across 2 Partitions
- The S3 bucket containing the objects is in the same region as the S3 Find and
  Forget Solution
- The total size of the data held in the column queried by Athena is 6.8GB
- The Find phase returns 15 objects which need to be modified
- The Forget phase uses 3 Fargate tasks with 4 vCPUs and 30GB of memory each,
  running concurrently for 60 minutes

| Service        | Spending | Notes                                                       |
| -------------- | -------- | ----------------------------------------------------------- |
| Amazon Athena  | \$0.03   | 6.8GB of data scanned                                       |
| AWS Fargate    | \$0.89   | 3 tasks x 4 vCPUs, 30GB memory x 1 hour                     |
| Amazon S3      | \$0.01   | \$0.01 of requests and data retrieval. \$0 of data transfer |
| Other services | \$0.05   | n/a                                                         |
| Total          | \$0.98   | n/a                                                         |

> Note: This estimate doesn't include the costs for Amazon VPC

### Scenario 2

This example shows how the charges would be calculated for a deletion job where:

- Your dataset is 750GB of Snappy compressed Parquet objects that are
  distributed across 1000 Partitions
- The S3 bucket containing the objects is in the same region as the S3 Find and
  Forget Solution
- The total size of the data held in the column queried by Athena is 10GB
- The Find phase returns 1000 objects which need to be modified
- The Forget phase uses 50 Fargate tasks with 4 vCPUs and 30GB of memory each,
  running concurrently for 45 minutes

| Service        | Spending | Notes                                                       |
| -------------- | -------- | ----------------------------------------------------------- |
| Amazon Athena  | \$0.05   | 10GB of data scanned                                        |
| AWS Fargate    | \$11.07  | 50 tasks x 4 vCPUs, 30GB memory x 0.75 hours                |
| Amazon S3      | \$0.01   | \$0.01 of requests and data retrieval. \$0 of data transfer |
| Other services | \$0.01   | n/a                                                         |
| Total          | \$11.14  | n/a                                                         |

> Note: This estimate doesn't include the costs for Amazon VPC

### Scenario 3

This example shows how the charges would be calculated for a deletion job where:

- Your dataset is 10TB of Snappy compressed Parquet objects that are distributed
  across 2000 Partitions
- The S3 bucket containing the objects is in the same region as the S3 Find and
  Forget Solution
- The total size of the data held in the column queried by Athena is 156GB
- The Find phase returns 11000 objects which need to be modified
- The Forget phase uses 100 Fargate tasks with 4 vCPUs and 30GB of memory each,
  running concurrently for 150 minutes

| Service        | Spending | Notes                                                       |
| -------------- | -------- | ----------------------------------------------------------- |
| Amazon Athena  | \$0.76   | 156GB of data scanned                                       |
| AWS Fargate    | \$73.82  | 100 tasks x 4 vCPUs, 30GB memory x 2.5 hours                |
| Amazon S3      | \$0.11   | \$0.11 of requests and data retrieval. \$0 of data transfer |
| Other services | \$1      | n/a                                                         |
| Total          | \$75.69  | n/a                                                         |

> Note: This estimate doesn't include the costs for Amazon VPC

### Scenario 4

This example shows how the charges would be calculated for a deletion job where:

- Your dataset is 50TB of Snappy compressed Parquet objects that are distributed
  across 5300 Partitions
- The S3 bucket containing the objects is in the same region as the S3 Find and
  Forget Solution
- The total size of the data held in the column queried by Athena is 671GB
- The Find phase returns 45300 objects which need to be modified
- The Forget phase uses 100 Fargate tasks with 4 vCPUs and 30GB of memory each,
  running concurrently for 10.5 hours

| Service        | Spending | Notes                                                       |
| -------------- | -------- | ----------------------------------------------------------- |
| Amazon Athena  | \$3.28   | 671GB of data scanned                                       |
| AWS Fargate    | \$310.03 | 100 tasks x 4 vCPUs, 30GB memory x 10.5 hours               |
| Amazon S3      | \$0.49   | \$0.49 of requests and data retrieval. \$0 of data transfer |
| Other services | \$3      | n/a                                                         |
| Total          | \$316.80 | n/a                                                         |

> Note: This estimate doesn't include the costs for Amazon VPC

### Scenario 5

This example shows how the charges would be calculated for a deletion job where:

- Your dataset is 100GB of Gzip compressed JSON objects that are distributed
  across 310 Partitions
- The S3 bucket containing the objects is in the same region as the S3 Find and
  Forget Solution
- The Find phase returns 3500 objects which need to be modified
- The Forget phase uses 50 Fargate tasks with 4 vCPUs and 30GB of memory each,
  running concurrently for 22 minutes

| Service        | Spending | Notes                                                       |
| -------------- | -------- | ----------------------------------------------------------- |
| Amazon Athena  | \$0.50   | 100GB of data scanned                                       |
| AWS Fargate    | \$5.31   | 50 tasks x 4 vCPUs, 30GB memory x 0.36 hours                |
| Amazon S3      | \$0.03   | \$0.03 of requests and data retrieval. \$0 of data transfer |
| Other services | \$0.05   | n/a                                                         |
| Total          | \$5.89   | n/a                                                         |

> Note: This estimate doesn't include the costs for Amazon VPC

[aws cloudformation]: https://aws.amazon.com/cloudformation/
[aws codebuild]: https://aws.amazon.com/codebuild/pricing/
[aws codepipeline]: https://aws.amazon.com/codepipeline/pricing/
[aws fargate pricing]: https://aws.amazon.com/fargate/pricing/
[aws fargate]: https://aws.amazon.com/fargate/pricing/
[aws glue pricing]: https://aws.amazon.com/glue/pricing/
[aws lambda pricing]: https://aws.amazon.com/lambda/pricing/
[aws lambda]: https://aws.amazon.com/lambda/pricing/
[aws privatelink pricing]: https://aws.amazon.com/privatelink/pricing/
[aws step functions pricing]: https://aws.amazon.com/step-functions/pricing/
[amazon api gateway pricing]: https://aws.amazon.com/api-gateway/pricing/
[amazon athena pricing]: https://aws.amazon.com/athena/pricing/
[amazon cloudfront pricing]: https://aws.amazon.com/cloudfront/pricing/
[amazon cognito pricing]: https://aws.amazon.com/cognito/pricing/
[amazon dynamodb pricing]: https://aws.amazon.com/dynamodb/pricing/
[amazon elastic container registry]: https://aws.amazon.com/ecr/pricing/
[amazon s3 pricing]: https://aws.amazon.com/s3/pricing/
[amazon sqs pricing]: https://aws.amazon.com/sqs/pricing/
[amazon vpc pricing]: https://aws.amazon.com/vpc/pricing/
[deletion job workflow]: ARCHITECTURE.md#deletion-job-workflow
[solution persistence layer]: ARCHITECTURE.md#persistence-layer
[using workgroups to control query access and costs]:
  https://docs.aws.amazon.com/athena/latest/ug/manage-queries-control-costs-with-workgroups.html
[vpc configuration]:
  USER_GUIDE.md#pre-requisite-Configuring-a-vpc-for-the-solution

[some VPC endpoints]:
[https://github.com/awslabs/amazon-s3-find-and-forget/blob/master/templates/vpc.yaml]
