# User Guide

This section describes how to install, configure and use the Amazon S3 Find and
Forget solution.

## Pre-requisite: Configuring a VPC for the Solution

The Fargate tasks used by this solution to perform deletions must be able to
access the following AWS services, either via an Internet Gateway or via
[VPC Endpoints]:
- Amazon S3
- Amazon DynamoDB
- Amazon CloudWatch (monitoring and logs)
- AWS ECR
- Amazon SQS

### Creating a New VPC

If you do not have an existing VPC you wish to use, a VPC template is available
as part of this solution which can be deployed separately to the main stack.
This template will create a VPC with private subnets and all the relevant VPC
Endpoints required by the Amazon S3 Find and Forget solution. To deploy this
template, use the VPC Template "Deploy to AWS button" in
[Deploying the Solution](#deploying-the-solution) then follow steps 5-9. The
**Outputs** tab will contain the subnets and security group IDs to use as inputs
for the main stack.

### Using an Existing VPC

If you wish to use an existing VPC in your account with the Amazon S3 Find and
Forget solution, you must ensure that when deploying the solution you select
subnets and security groups which permit access to these services.

You can obtain your subnet and security group IDs from the AWS Console or by
using the AWS CLI. If using the AWS CLI, you can use the following command
to get a list of VPCs:

```bash
aws ec2 describe-vpcs \
  --query 'Vpcs[*].{ID:VpcId,Name:Tags[?Key==`Name`].Value | [0], IsDefault: IsDefault}'
```

Once you have found the VPC you wish to use, to get a list of subnets and
security groups in that VPC:

```bash
export VPC_ID=<chosen-vpc-id>
aws ec2 describe-subnets \
  --filter Name=vpc-id,Values="$VPC_ID" \
  --query 'Subnets[*].{ID:SubnetId,Name:Tags[?Key==`Name`].Value | [0],AZ:AvailabilityZone}'
aws ec2 describe-security-groups \
  --filter Name=vpc-id,Values="$VPC_ID" \
  --query 'SecurityGroups[*].{ID:GroupId,Name:GroupName}'
```

## Deploying the Solution

The solution is deployed as an
[AWS CloudFormation](https://aws.amazon.com/cloudformation) template.

Your access to the AWS account must have IAM permissions to launch AWS
CloudFormation templates that create IAM roles and to create the solution
resources.

> **Note** You are responsible for the cost of the AWS services used while
> running this solution. For full details, see the pricing pages for each AWS
> service you will be using in this sample. Prices are subject to change.

1. Deploy the latest CloudFormation template by following the link below for
your preferred AWS region:

|Region|Launch Template|VPC Template|
|------|---------------|------------|
|**US East (N. Virginia)** (us-east-1) | [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-us-east-1.s3.us-east-1.amazonaws.com/amazon-s3-find-and-forget/latest/main.template)| [![Launch the Amazon S3 Find and Forget VPC Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/new?stackName=S3F2-VPC&templateURL=https://solution-builders-us-east-1.s3.us-east-1.amazonaws.com/amazon-s3-find-and-forget/latest/vpc.yaml)|
|**US East (Ohio)** (us-east-2) | [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=us-east-2#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-us-east-2.s3.us-east-2.amazonaws.com/amazon-s3-find-and-forget/latest/main.template)| [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=us-east-2#/stacks/new?stackName=S3F2-VPC&templateURL=https://solution-builders-us-east-2.s3.us-east-2.amazonaws.com/amazon-s3-find-and-forget/latest/vpc.yaml)|
|**US West (Oregon)** (us-west-2) | [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=us-west-2#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-us-west-2.s3.us-west-2.amazonaws.com/amazon-s3-find-and-forget/latest/main.template)| [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=us-west-2#/stacks/new?stackName=S3F2-VPC&templateURL=https://solution-builders-us-west-2.s3.us-west-2.amazonaws.com/amazon-s3-find-and-forget/latest/vpc.yaml)|
|**Asia Pacific (Seoul)** (ap-northeast-2) | [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=ap-northeast-2#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-ap-northeast-2.s3.ap-northeast-2.amazonaws.com/amazon-s3-find-and-forget/latest/main.template)| [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=ap-northeast-2#/stacks/new?stackName=S3F2-VPC&templateURL=https://solution-builders-ap-northeast-2.s3.ap-northeast-2.amazonaws.com/amazon-s3-find-and-forget/latest/vpc.yaml)|
|**Asia Pacific (Sydney)** (ap-southeast-2) | [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=ap-southeast-2#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-ap-southeast-2.s3.ap-southeast-2.amazonaws.com/amazon-s3-find-and-forget/latest/main.template)| [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=ap-southeast-2#/stacks/new?stackName=S3F2-VPC&templateURL=https://solution-builders-ap-southeast-2.s3.ap-southeast-2.amazonaws.com/amazon-s3-find-and-forget/latest/vpc.yaml)|
|**Asia Pacific (Tokyo)** (ap-northeast-1) | [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=ap-northeast-1#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-ap-northeast-1.s3.ap-northeast-1.amazonaws.com/amazon-s3-find-and-forget/latest/main.template)| [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=ap-northeast-1#/stacks/new?stackName=S3F2-VPC&templateURL=https://solution-builders-ap-northeast-1.s3.ap-northeast-1.amazonaws.com/amazon-s3-find-and-forget/latest/vpc.yaml)|
|**EU (Ireland)** (eu-west-1) | [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=eu-west-1#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-eu-west-1.s3.eu-west-1.amazonaws.com/amazon-s3-find-and-forget/latest/main.template)| [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=eu-west-1#/stacks/new?stackName=S3F2-VPC&templateURL=https://solution-builders-eu-west-1.s3.eu-west-1.amazonaws.com/amazon-s3-find-and-forget/latest/vpc.yaml)|

2. If prompted, login using your AWS account credentials.
3. You should see a screen titled "*Create Stack*" at the "*Specify template*"
   step. The fields specifying the CloudFormation template are pre-populated.
   Choose the *Next* button at the bottom of the page.
4. On the "*Specify stack details*" screen you should provide values for the
   following parameters of the CloudFormation stack:
   * **Stack Name:** (Default: S3F2) This is the name that is used to refer to
   this stack in CloudFormation once deployed.
   * **AdminEmail:** The email address you wish to setup as the initial
   user of this Amazon S3 Find and Forget deployment.
   * **SafeMode:** (Default: true) Whether to operate in safe mode. Whilst Safe
   Mode is set to true, updated objects will be written to a temporary bucket
   instead of overwriting the original object. For more information see
   [Disabling Safe Mode](#disabling-safe-mode)
   * **JobDetailsRetentionDays:** (Default: 0) How long job records should
   remain in the Job table for. Use 0 to retain logs indefinitely. **Note:**
   If the retention setting is changed it will only apply to *new* deletion jobs.
   Existing deletion jobs will retain the TTL at the time they were ran.
   * **VpcSecurityGroups:** List of security group IDs to apply to Fargate
   deletion tasks. For more information on how to obtain these IDs, see
   [Obtaining VPC Information](#obtaining-vpc-information)
   * **VpcSubnets:** List of subnets to run Fargate deletion tasks in.
   For more information on how to obtain these IDs, see
   [Obtaining VPC Information](#obtaining-vpc-information)
   
   The following parameters are optional and allow further customisation of the
   solution if required:
   
   * **CreateCloudFrontDistribution:** (Default: true) Creates a CloudFront
   distribution for accessing the web interface of the solution.
   * **AccessControlAllowOriginOverride:** (Default: false) Allows overriding
   the origin from which the API can be called. If 'false' is provided, the
   API will only accept requests from the Web UI origin.
   * **AthenaConcurrencyLimit:** (Default: 20) The number of concurrent Athena
   queries the solution will run when scanning your data lake.
   * **DeletionTasksMaxNumber:** (Default: 3)  Max number of concurrent 
   Fargate tasks to run when performing deletions.
   * **DeletionTaskCPU:** (Default: 4096) Fargate task CPU limit. For more info
   see [Fargate Configuration]
   * **DeletionTaskMemory:** (Default: 30720) Fargate task memory limit. For
   more info see [Fargate Configuration]
   * **QueryExecutionWaitSeconds:** (Default: 3) How long to wait when
   checking if an Athena Query has completed.
   * **QueryQueueWaitSeconds:** (Default: 3)  How long to wait when
   checking if there the current number of executing queries is less than the
   specified concurrency limit.
   * **ForgetQueueWaitSeconds:** (Default: 30) How long to wait when
   checking if the Forget phase is complete
   * **AthenaWorkGroup:** (Default: primary) The Athena work group that should
   be used for when the solution runs Athena queries.
   * **EnableContainerInsights:** (Default: false) Whether to enable CloudWatch
   Container Insights.
   * **PreBuiltArtefactsBucketOverride:** (Default: false) Overrides the default
   Bucket containing Front-end and Back-end pre-built artefacts. Use this
   if you are using a customised version of these artefacts.
   * **ResourcePrefix:** (Default: S3F2) Resource prefix to apply to resource
   names when creating statically named resources.

   When completed, click *Next*
5. [Configure stack options](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/cfn-console-add-tags.html) if desired, then click *Next*.
6. On the review you screen, you must check the boxes for:
   * "*I acknowledge that AWS CloudFormation might create IAM resources*" 
   * "*I acknowledge that AWS CloudFormation might create IAM resources
   with custom names*" 

   These are required to allow CloudFormation to create a Role to allow access
   to resources needed by the stack and name the resources in a dynamic way.
7. Choose *Create Change Set* 
8. On the *Change Set* screen, click *Execute* to launch your stack.
   * You may need to wait for the *Execution status* of the change set to
   become "*AVAILABLE*" before the "*Execute*" button becomes available.
9. Wait for the CloudFormation stack to launch. Completion is indicated when
   the "Stack status" is "*CREATE_COMPLETE*".
   * You can monitor the stack creation progress in the "Events" tab.
10. Note the *WebUIUrl* displayed in the *Outputs* tab for the stack. This is
   used to access the application.

## Configuring Data Mappers

After [Deploying the Solution](#deploying-the-solution), your first step should
be to configure one or more data mappers which will connect your data to the
solution. Identify the S3 Bucket containing the data you wish to connect to the
solution and ensure you have defined a table in your data catalog and that all
existing (and future partitions as they are created) are known to the Data
Catalog. Currently AWS Glue is the only supported data catalog provider. For
more information on defining your data in the Glue Data Catalog, see
[Defining Glue Tables]. You must define your Table in the Glue Data Catalog in
the same region and account as the S3 Find and Forget solution.

1. Access the application UI via the **WebUIUrl** displayed in the *Outputs* tab
for the stack.
2. Choose **Data Mappers** from the menu then choose **Create Data Mapper** 
3. On the Create Data Mapper page input a **Name** to uniquely identify this
Data Mapper. Select a **Query Executor Type** then choose the **Database** and
**Table** in your data catalog which describes the target data in S3.
A list of columns will be displayed for the chosen Table. From the
list, choose the column(s) the solution should use to to find items in the
data which should be deleted. For example, if your table has three columns
named **customer_id**, **description** and **created_at** and you want to
search for items using the **customer_id**, you should choose only the
**customer_id** column from this list. Once you have chosen the column(s),
choose **Create Data Mapper**.
4. A message will be displayed advising you to update the S3 Bucket Policy
for the S3 Bucket referenced by the newly created data mapper. See
[Granting Access to Data](#granting-access-to-data) for more information
on how to do this. Choose **Return to Data Mappers**.

## Granting Access to Data

After configuring a data mapper you must ensure that the S3 Find and Forget
solution has the required level of access to the S3 location the data mapper
refers to. The recommended way to achieve this is through the use of
[S3 Bucket Policies].

> **Note:** AWS IAM uses an eventual consistency moodel and therefore any change
> you make to IAM, Bucket or KMS Key policies may take time to become visible.
> Ensure that the permissions changes have been propagated to all endpoints
> before starting a job.

### Updating your Bucket Policy

To update the S3 bucket policy to grant **read** access to the IAM role used by
Amazon Athena, and **write** access to the IAM role used by AWS Fargate, follow
these steps:

1. Access the application UI via the **WebUIUrl** displayed in the *Outputs* tab
for the stack.
2. Choose **Data Mappers** from the menu then choose the radio button for the
relevant data mapper from the **Data Mappers** list.
3. Choose **Generate Access Policies** and follow the instructions on the
**Bucket Access** tab to update the bucket policy. If you already have a
bucket policy in place, add the statements shown to your existing bucket policy
rather than replacing it completely. If your data is encrypted with an
**Customer Managed CMK** rather than an **AWS Managed CMK**, see
[Data Encrypted with Customer Managed CMK](#data-encrypted-with-a-customer-managed-cmk)
to grant the solution access to the Customer Managed CMK. If the bucket and/or
Customer Managed CMK reside in a different account, see
[Cross Account Buckets/CMKs](#cross-account-buckets-and-cmks) **after** you
have granted any required Customer Managed CMK access. For more information on
using Server-Side Encryption (SSE) with S3, see [Using SSE with CMKs].

### Data Encrypted with a Customer Managed CMK

Where the data you are connecting to the solution is encrypted with an Customer
Managed CMK rather than an AWS Managed CMK, you must also grant the Athena
and Fargate IAM roles access to use the key so that the data can be decrypted
when reading, re-encrypted when writing.

Once you have updated the bucket policy as described in
[Updating the Bucket Policy](#updating-the-bucket-policy), choose
the **KMS Access** tab from the **Generate Access Policies** modal window and
follow the instructions to update the key policy with the provided statements.
The statements provided are for use when using the **policy view** in the AWS
console or making updates to the key policy via the CLI, CloudFormation or the
API. If you wish, to use the **default view** in th AWS console, add the
**Principals** in the provided statements as **key users**. For more
information, see [How to Change a Key Policy].

### Cross Account Buckets and CMKs  

Where the bucket referenced by a data mapper is in a different account to the
deployed S3 Find and Forget solution, and/or the Customer Managed CMK use to
encrypt data via SSE is in a different account, you also need to update the
Athena/Fargate roles to grant them access to bucket/keys.

Once you have updated the bucket policy and any key policies as described in
[Updating the Bucket Policy](#updating-the-bucket-policy) and [Data Encrypted
with a Customer Managed CMK](#data-encrypted-with-a-customer-managed-cmk), 
choose the **KMS Access** tab from the **Generate Access Policies** modal
window and follow the instructions to add the provided inline policies to the
Athena and Fargate IAM roles with the provided statements. For more information,
see [Cross Account S3 Access] and [Cross Account CMK Access].

## Adding to the Deletion Queue
*TODO*

## Running a Deletion Job
*TODO*
Choose Start
Event History
List of Events
To optimise costs
Ref Job statuses

### Deletion Job Statuses

The list of possible job statuses is as follows:

- `QUEUED`: The job has been accepted but has yet to start. Jobs are started
  asynchronously by a Lambda invoked by the [DynamoDB event stream][DynamoDB Streams]
  for the Jobs table.
- `RUNNING`: The job is still in progress.
- `FORGET_COMPLETED_CLEANUP_IN_PROGRESS`: The job is still in progress.
- `COMPLETED`: The job finished successfully.
- `COMPLETED_CLEANUP_FAILED`: The job finished successfully however the
  deletion queue items could not be removed. You should manually remove these
  or leave them to be removed on the next job
- `FORGET_PARTIALLY_FAILED`: The job finished but one or more objects could not
  be updated. The Deletion DLQ for messages will contain a message per object
  that could not be updated.
- `FIND_FAILED`: The job failed during the Find phase as there was an issue
  querying one or more data mappers.
- `FORGET_FAILED`: The job failed during the Forget phase as there was an issue
  running the Fargate tasks.
- `FAILED`: An unknown error occurred during the Find and Forget workflow, for
  example, the Step Functions execution timed out or the execution was manually
  cancelled.

For more information on how to resolve statuses indicative of errors, consult
the [Troubleshooting] guide.

### Event Types

The list of events is as follows:

- `JobStarted`: 
- `FindPhaseStarted`: 
- `FindPhaseEnded`: 
- `FindPhaseFailed`: 
- `ForgetPhaseStarted`: 
- `ForgetPhaseEnded`: 
- `ForgetPhaseFailed`: 
- `CleanupSucceeded`: 
- `CleanupFailed`: 
- `CleanupSkipped`: 
- `QuerySucceeded`: 
- `QueryFailed`: 
- `ObjectUpdated`: 
- `ObjectUpdateFailed`: 
- `Exception`: 


## Adjusting Performance Configuration
*TODO*

## Updating the Stack
*TODO*

[Troubleshooting]: TROUBLESHOOTING.md
[Fargate Configuration]: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/AWS_Fargate.html#fargate-tasks-size
[VPC Endpoints]: https://docs.aws.amazon.com/vpc/latest/userguide/vpc-endpoints.html
[DynamoDB Streams]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html
[Defining Glue Tables]: https://docs.aws.amazon.com/glue/latest/dg/tables-described.html
[S3 Bucket Policies]: https://docs.aws.amazon.com/AmazonS3/latest/dev/using-iam-policies.html
[Using SSE with CMKs]: https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingKMSEncryption.html
[Customer Master Keys]: https://docs.aws.amazon.com/kms/latest/developerguide/concepts.html#master_keys
[How to Change a Key Policy]: https://docs.aws.amazon.com/kms/latest/developerguide/key-policy-modifying.html#key-policy-modifying-how-to
[Cross Account S3 Access]: https://docs.aws.amazon.com/AmazonS3/latest/dev/example-walkthroughs-managing-access-example2.html
[Cross Account KMS Access]: https://docs.aws.amazon.com/kms/latest/developerguide/key-policy-modifying-external-accounts.html
