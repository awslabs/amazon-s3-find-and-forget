# User Guide

This section describes how to install, configure and use the Amazon S3 Find and
Forget solution.

## Index
* [Pre-requisite: Configuring a VPC](#pre-requisite-configuring-a-vpc-for-the-solution)
    * [Creating a New VPC](#creating-a-new-vpc)
    * [Using an Existing VPC](#using-an-existing-vpc)
* [Deploying the Solution](#deploying-the-solution)
* [Configuring Data Mappers](#configuring-data-mappers)
* [Granting Access to Data](#granting-access-to-data)
    * [Updating Your Bucket Policy](#updating-your-bucket-policy)
    * [Data Encrypted with Customer Managed CMKs](#data-encrypted-with-a-customer-managed-cmk)
    * [Cross Account Buckets and CMKs](#cross-account-buckets-and-cmks)
* [Adding to the Deletion Queue](#adding-to-the-deletion-queue)
* [Running a Deletion Job](#running-a-deletion-job)
    * [Deletion Job Statuses](#deletion-job-statuses)
    * [Deletion Job Event Types](#deletion-job-event-types)
* [Adjusting Configuration](#adjusting-configuration)
* [Updating the Solution](#updating-the-solution)
    * [Identify current solution version](#identify-current-solution-version)
    * [Identify the Stack URL to deploy](#identify-the-stack-url-to-deploy)
    * [Minor Upgrades: Perform CloudFormation Stack Update](#minor-upgrades-perform-cloudformation-stack-update)
    * [Major Upgrades: Manual Rolling Deployment](#major-upgrades-manual-rolling-deployment)
* [Deleting the Solution](#deleting-the-solution)


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

1. Deploy the latest CloudFormation template using the AWS Console by choosing the "*Launch Template*" button below for your preferred AWS region. If you wish to [deploy using the AWS CLI] instead, you can refer to the "*Template Link*" to download the template files.

|Region|Launch Template|Template Link|Launch VPC Template|VPC Template Link|
|-|-|-|-|-|
|**US East (N. Virginia)** (us-east-1) |[![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-us-east-1.s3.us-east-1.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml)| [Link](https://solution-builders-us-east-1.s3.us-east-1.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml)| [![Launch the Amazon S3 Find and Forget VPC Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/new?stackName=S3F2-VPC&templateURL=https://solution-builders-us-east-1.s3.us-east-1.amazonaws.com/amazon-s3-find-and-forget/latest/vpc.yaml)| [Link](https://solution-builders-us-east-1.s3.us-east-1.amazonaws.com/amazon-s3-find-and-forget/latest/vpc.yaml)|
|**US East (Ohio)** (us-east-2) | [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=us-east-2#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-us-east-2.s3.us-east-2.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml)| [Link](https://solution-builders-us-east-2.s3.us-east-2.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml)|[![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=us-east-2#/stacks/new?stackName=S3F2-VPC&templateURL=https://solution-builders-us-east-2.s3.us-east-2.amazonaws.com/amazon-s3-find-and-forget/latest/vpc.yaml)| [Link](https://solution-builders-us-east-2.s3.us-east-2.amazonaws.com/amazon-s3-find-and-forget/latest/vpc.yaml)
|**US West (Oregon)** (us-west-2) | [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=us-west-2#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-us-west-2.s3.us-west-2.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml)| [Link](https://solution-builders-us-west-2.s3.us-west-2.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml)| [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=us-west-2#/stacks/new?stackName=S3F2-VPC&templateURL=https://solution-builders-us-west-2.s3.us-west-2.amazonaws.com/amazon-s3-find-and-forget/latest/vpc.yaml)| [Link](https://solution-builders-us-west-2.s3.us-west-2.amazonaws.com/amazon-s3-find-and-forget/latest/vpc.yaml)|
|**Asia Pacific (Seoul)** (ap-northeast-2) | [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=ap-northeast-2#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-ap-northeast-2.s3.ap-northeast-2.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml)| [Link](https://solution-builders-ap-northeast-2.s3.ap-northeast-2.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml)| [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=ap-northeast-2#/stacks/new?stackName=S3F2-VPC&templateURL=https://solution-builders-ap-northeast-2.s3.ap-northeast-2.amazonaws.com/amazon-s3-find-and-forget/latest/vpc.yaml)| [Link](https://solution-builders-ap-northeast-2.s3.ap-northeast-2.amazonaws.com/amazon-s3-find-and-forget/latest/vpc.yaml)|
|**Asia Pacific (Sydney)** (ap-southeast-2) | [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=ap-southeast-2#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-ap-southeast-2.s3.ap-southeast-2.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml)| [Link](https://solution-builders-ap-southeast-2.s3.ap-southeast-2.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml)| [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=ap-southeast-2#/stacks/new?stackName=S3F2-VPC&templateURL=https://solution-builders-ap-southeast-2.s3.ap-southeast-2.amazonaws.com/amazon-s3-find-and-forget/latest/vpc.yaml)| [Link](https://solution-builders-ap-southeast-2.s3.ap-southeast-2.amazonaws.com/amazon-s3-find-and-forget/latest/vpc.yaml)|
|**Asia Pacific (Tokyo)** (ap-northeast-1) | [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=ap-northeast-1#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-ap-northeast-1.s3.ap-northeast-1.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml)| [Link](https://solution-builders-ap-northeast-1.s3.ap-northeast-1.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml)| [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=ap-northeast-1#/stacks/new?stackName=S3F2-VPC&templateURL=https://solution-builders-ap-northeast-1.s3.ap-northeast-1.amazonaws.com/amazon-s3-find-and-forget/latest/vpc.yaml)|[Link](https://solution-builders-ap-northeast-1.s3.ap-northeast-1.amazonaws.com/amazon-s3-find-and-forget/latest/vpc.yaml)|
|**EU (Ireland)** (eu-west-1) | [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=eu-west-1#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-eu-west-1.s3.eu-west-1.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml)| [Link](https://solution-builders-eu-west-1.s3.eu-west-1.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml)| [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=eu-west-1#/stacks/new?stackName=S3F2-VPC&templateURL=https://solution-builders-eu-west-1.s3.eu-west-1.amazonaws.com/amazon-s3-find-and-forget/latest/vpc.yaml)| [Link](https://solution-builders-eu-west-1.s3.eu-west-1.amazonaws.com/amazon-s3-find-and-forget/latest/vpc.yaml)|

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
   * **RetainDynamoDBTables:** (Default: true) Wheter to retain the DynamoDB tables upon Stack Update and Stack Deletion.

   When completed, click *Next*
5. [Configure stack options](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/cfn-console-add-tags.html) if desired, then click *Next*.
6. On the review screen, you must check the boxes for:
   * "*I acknowledge that AWS CloudFormation might create IAM resources*" 
   * "*I acknowledge that AWS CloudFormation might create IAM resources
   with custom names*" 

   These are required to allow CloudFormation to create a Role to allow access
   to resources needed by the stack and name the resources in a dynamic way.
7. Choose *Create Change Set* 
8. On the *Change Set* screen, click *Execute* to launch your stack.
   * You may need to wait for the *Execution status* of the change set to
   become "*AVAILABLE*" before the "*Execute*" button becomes available.
9. Wait for the CloudFormation stack to launch. Completion is indicated when the "Stack status" is "*CREATE_COMPLETE*".
   * You can monitor the stack creation progress in the "Events" tab.
10. Note the *WebUIUrl* displayed in the *Outputs* tab for the stack. This is used to access the application.

## Configuring Data Mappers

After [Deploying the Solution](#deploying-the-solution), your first step should
be to configure one or more [data mappers](ARCHITECTURE.md#data-mappers) which
will connect your data to the solution. Identify the S3 Bucket containing the
data you wish to connect to the solution and ensure you have defined a table in
your data catalog and that all existing and future partitions (as they are
created) are known to the Data Catalog. Currently AWS Glue is the only supported
data catalog provider. For more information on defining your data in the Glue
Data Catalog, see [Defining Glue Tables]. You must define your Table in the
Glue Data Catalog in the same region and account as the S3 Find and Forget
solution.

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

You can also create Data Mappers directly via the API. For more information,
see the [API Documentation].

## Granting Access to Data

After configuring a data mapper you must ensure that the S3 Find and Forget
solution has the required level of access to the S3 location the data mapper
refers to. The recommended way to achieve this is through the use of
[S3 Bucket Policies].

> **Note:** AWS IAM uses an [eventual consistency model](https://docs.aws.amazon.com/IAM/latest/UserGuide/troubleshoot_general.html#troubleshoot_general_eventual-consistency)
> and therefore any change you make to IAM, Bucket or KMS Key policies may
> take time to become visible. Ensure you have allowed time for permissions
> changes to propagate to all endpoints before starting a job. If your job
> fails with a status of FIND_FAILED and the `QueryFailed` events indicate
> S3 permissions issues, you may need to wait for the permissions changes
> to propagate.

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
   to grant the solution access to the Customer Managed CMK. If the bucket
   and/or Customer Managed CMK reside in a different account, see
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

Once your Data Mappers are configured, you can begin adding "Matches" to the
[Deletion Queue](ARCHITECTURE.md#deletion-queue).

1. Access the application UI via the **WebUIUrl** displayed in the *Outputs* tab
   for the stack.
2. Choose **Deletion Queue** from the menu then choose **Add Match to the
   Deletion Queue**.
3. Input a **Match**, which is the value to search for in your data mappers.
   If you wish to search for the match from all data mappers choose
   **All Data Mappers**, otherwise choose **Select your Data Mappers** then
   select the relevant data mappers from the list.
4. Choose **Add Item to the Deletion Queue** and confirm you can see the
   match in the Deletion Queue.

You can also add matches to the Deletion Queue directly via the API. For more
information, see the [API Documentation].

When the next deletion job runs, the solution will scan the configured columns
of your data for any occurrences of the Matches present in the queue at the
time the job starts and remove any items where one of the Matches is present.

If across all your data mappers you can find all items related to a single
logical entity using the same value, you only need to add one Match value to the
deletion queue to delete that logical entity from all data mappers.

If the value used to identify a single logical entity is not consistent across
your data mappers, you should add an item to the deletion queue **for each
distinct value** which identifies the logical entity, selecting the specific
data mapper(s) to which that value is relevant.

If you make a mistake when adding a Match to the deletion queue, you can remove
that match from the queue as long as there is no job running. Once a job has
started no items can be removed from the deletion queue until the running job
has completed. You may continue to add matches to the queue whilst a job is
running, but only matches which were present when the job started will be
processed by that job. Once a job completes, only the matches that job has
processed will be removed from the queue.

In order to facilitate different teams using a single deployment within an
organisation, the same match can be added to the deletion queue more than once.
When the job executes, it will merge the lists of data mappers for duplicates
in the queue.

## Running a Deletion Job

Once you have configured your data mappers and added one or more items to the
deletion queue, you can stat a job.

1. Access the application UI via the **WebUIUrl** displayed in the *Outputs* tab
   for the stack.
2. Choose **Deletion Jobs** from the menu and ensure there are no jobs
   currently running. Choose **Start a Deletion Job** and review the settings
   displayed on the screen. For more information on how to edit these settings,
   see [Adjusting Configuration](#adjusting-configuration).
3. If you are happy with the current solution configuration choose **Start
   a Deletion Job**. The job details page should be displayed.
   
You can also start jobs directly via the API. For more information, see the
[API Documentation].
   
Once a job has started, you can leave the page and return to view its progress
at point by choosing the job ID from the Deletion Jobs list. The job details
page will automatically refresh and to display the current status and
statistics for the job. For more information on the possible statuses and
their meaning, see [Deletion Job Statuses](#deletion-job-statuses).

Job events are continuously emitted whilst a job is running. These events are
used to update the status and statistics for the job. You can view all the
emitted events for a job in the **Job Events** table. Whilst a job is running,
the **Load More** button will continue to be displayed even if no new events
have been received. Once a job has finished, the **Load More** button will
disappear once you have loaded all the emitted events. For more information
on the events which can be emitted during a job, see [Deletion Job Event
Types](#deletion-job-event-types)

To optimise costs, it is best practice when using the solution to start jobs
on a regular schedule, rather than every time a single item is added to the
Deletion Queue. This is because the marginal cost of the Find phase when
deleting an additional item from the queue is far less that re-executing
the Find phase (where the data mappers searched are the same). Similarly, the
marginal cost of removing an additional match from an object is negligible when
there is already at least 1 match present in the object contents.

### Deletion Job Statuses

The list of possible job statuses is as follows:

- `QUEUED`: The job has been accepted but has yet to start. Jobs are started
  asynchronously by a Lambda invoked by the [DynamoDB event stream][DynamoDB
  Streams] for the Jobs table.
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

### Deletion Job Event Types

The list of events is as follows:

- `JobStarted`: Emitted when the deletion job state machine first starts.
  Causes the status of the job to transition from `QUEUED` to `RUNNING`
- `FindPhaseStarted`: Emitted when the deletion job has purged any messages
  from the query and object queues and is ready to be searching for data.
- `FindPhaseEnded`: Emitted when all queries have executed and written their
  results to the objects queue.
- `FindPhaseFailed`: Emitted when one or more queries fail. Causes the status
  to transition to `FIND_FAILED`.
- `ForgetPhaseStarted`: Emitted when the Find phase has completed successfully
  and the Forget phase is starting.
- `ForgetPhaseEnded`: Emitted when the Forget phase has completed. If the
  Forget phase completes with no errors, this event causes the status to
  transition to `FORGET_COMPLETED_CLEANUP_IN_PROGRESS`. If the
  Forget phase completes but there was an error updating one or more objects,
  this causes the status to transition to `FORGET_PARTIALLY_FAILED`.
- `ForgetPhaseFailed`: Emitted when there was an issue running the Fargate
  tasks. Causes the status to transition to `FORGET_FAILED`.
- `CleanupSucceeded`: The **final** event emitted when a job has executed
  successfully and the Deletion Queue has been cleaned up. Causes the status
  to transition to `COMPLETED`.
- `CleanupFailed`: The **final** event emitted when the job executed
  successfully but there was an error removing the processed matches from
  the Deletion Queue. Causes the status to transition to
  `COMPLETED_CLEANUP_FAILED`.
- `CleanupSkipped`: Emitted when the job is finalising and the job status
  is one of `FIND_FAILED`, `FORGET_FAILED` or `FAILED`.
- `QuerySucceeded`: Emitted whenever a single query executes successfully.
- `QueryFailed`: Emitted whenever a single query fails.
- `ObjectUpdated`: Emitted whenever an updated object is written to S3 and
  any associated deletions are complete.
- `ObjectUpdateFailed`: Emitted whenever an object cannot be updated or an
  associated deletion fails.
- `Exception`: Emitted whenever a generic error occurs during the
  job execution. Causes the status to transition to `FAILED`.

## Adjusting Configuration

There are several parameters to set when [Deploying the
Solution](#deploying-the-solution) which affect the behaviour of the solution
in terms of data retention and performance:

* `AthenaConcurrencyLimit`: Increasing the number of concurrent queries that
 should be executed will decrease the total time
  spent performing the Find phase. You should not increase this value beyond
  your account Service Quota for concurrent DML queries, and should ensure
  that the value set takes into account any other Athena DML queries that
  may be executing whilst a job is running.
* `DeletionTasksMaxNumber`: Increasing the number of concurrent tasks that
  should consume messages from the object queue will decrease the total time
  spent performing the Forget phase.
* `QueryExecutionWaitSeconds`: Decreasing this value will decrease the length
  of time between each check to see whether a query has completed. You should
  aim to set this to the "ceiling function" of your average query time. For
  example, if you average query takes 3.2 seconds, set this to 4.
* `QueryQueueWaitSeconds`: Decreasing this value will decrease the length of
  time between each check to see whether additional queries can be scheduled
  during the Find phase. If your jobs fail due to exceeding the Step Functions
  execution history quota, you may have set this value to low and should
  increase it to allow more queries to be scheduled after each check.
* `ForgetQueueWaitSeconds`: Decreasing this value will decrease the length of
  time between each check to see whether the Fargate object queue is empty.
  If your jobs fail due to exceeding the Step Functions execution history quota,
  you may have set this value to low.
* `JobDetailsRetentionDays`: Changing this value will change how long records
  job details and events are retained for. Set this to 0 to retain them
  indefinitely.

The values for these parameters are stored in an SSM Parameter Store String
Parameter named  `/s3f2/S3F2-Configuration` as a JSON object. The recommended
approach for updating these values is to perform a [Stack Update](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-direct.html)
and change the relevant parameters for the stack.

It is possible to [update the SSM Parameter][Updating an SSM Parameter] directly
however this is not a recommended approach. **You should not alter the
structure or data types of the configuration JSON object.**
 
Once updated, the configuration will affect any **future** job executions.
In progress and previous executions will **not** be affected. The current
configuration values are displayed when confirming that you wish to start a job.

You can only update the vCPUs/memory allocated to Fargate tasks by performing a
stack update. For more information, see [Updating the Solution](#updating-the-solution).

## Updating the Solution

In order to benefit from the latest features and security updates, you should aim to always have the latest solution version deployed. To find out what the latest version is and what has changed since your currently deployed version, check the [Changelog].

There are two ways to update the solution depending on the type of upgrade between versions. When the new version is a *minor* upgrade (for instance, from version 3.45 to 3.67) it is recommended to deploy via CloudFormation Stack Update. When the new version is a *major* upgrade (for instance, from 2.34 to 3.0), it is recommended to perform a manual rolling deployment.

Major version releases are rare and only occur when a non-backwards compatible change is made to the solution.

### Identify current solution version

To find out which version is currently deployed, check the value of the `SolutionVersion` output for the solution stack. The solution version is also shown on the Dashboard of the Web UI.

### Identify the Stack URL to deploy

After reviewing the [Changelog], obtain the `Template Link` url of the latest version from ["Deploying the Solution"](#deploying-the-solution) (it will be similar to `https://solution-builders-us-east-1.s3.us-east-1.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml`). If you wish to deploy a specific version rather than the latest version, replace `latest` from the url with the chosen version, for instance `https://solution-builders-us-east-1.s3.us-east-1.amazonaws.com/amazon-s3-find-and-forget/v0.2/template.yaml`.

### Minor Upgrades: Perform CloudFormation Stack Update

To deploy via AWS Console:
1. Open the [CloudFormation Console Page] and choose the Solution by selecting to the stack's radio button, then choose "Update"
2. Choose "Replace current template" and then input the template URL for the version you wish to deploy in the "Amazon S3 URL" textbox, then choose "Next"
3. On the *Stack Details* screen, review the Parameters and then choose "Next"
4. On the *Configure stack options* screen, choose "Next"
5. On the *Review stack* screen, you must check the boxes for:
   * "*I acknowledge that AWS CloudFormation might create IAM resources*"
   * "*I acknowledge that AWS CloudFormation might create IAM resources
   with custom names*"
   * "*I acknowledge that AWS CloudFormation might require the following capability: CAPABILITY_AUTO_EXPAND*"

   These are required to allow CloudFormation to create a Role to allow access
   to resources needed by the stack and name the resources in a dynamic way.
6. Choose "Update stack" to start the stack update.
7. Wait for the CloudFormation stack to finish updating. Completion is indicated when the "Stack status" is "*UPDATE_COMPLETE*".

To deploy via the AWS CLI [consult the documentation](https://docs.aws.amazon.com/cli/latest/reference/cloudformation/update-stack.html).


### Major Upgrades: Manual Rolling Deployment

In a manual rolling deployment you will: create a new stack from scratch, export the data from the old stack to the new stack, migrate consumers to new API and Web UI URLs, and then delete the old stack.

1. Deploy a new instance of the Solution by following the instructions contained in the ["Deploying the Solution" section](#deploying-the-solution). Make sure you use unique values for Stack Name and ResourcePrefix parameter compared to the existing stack.
2. Migrate Data from DynamoDB to ensure the new stack contains the necessary configuration related to Data Mappers and settings. When both stacks are deployed in the same account and region, the simpler way to migrate is via [On-Demand Backup and Restore]. If the stacks are deployed in different regions or accounts, you can use [AWS Data Pipeline].
3. Ensure that all the bucket policies for the Data Mappers are in place for the new stack. See the ["Granting Access to Data" section](#granting-access-to-data) for steps to do this.
4. Review the [Changelog] for changes that may affect how you use the new deployment. This may require you to make changes to any software you have that interacts with the solution's API.
5. Once all the consumers are migrated to the new stack (API and Web UI), delete the old stack. 

## Deleting the Solution

To delete a stack via AWS Console:
1. Open the [CloudFormation Console Page] and choose the solution stack, then choose "Delete"
2. When the confirmation modal shows up, choose "Delete stack".
3. Wait for the CloudFormation stack to finish updating. Completion is indicated when the "Stack status" is "*DELETE_COMPLETE*".

To delete a stack via the AWS CLI [consult the documentation](https://docs.aws.amazon.com/cli/latest/reference/cloudformation/delete-stack.html).


[API Documentation]: api/README.md
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
[Updating an SSM Parameter]: https://docs.aws.amazon.com/systems-manager/latest/userguide/sysman-paramstore-cli.html
[deploy using the aws cli]: https://docs.aws.amazon.com/cli/latest/reference/cloudformation/deploy/index.html
[cloudformation console page]: https://console.aws.amazon.com/cloudformation/home
[changelog]: ../CHANGELOG.md
[on-demand backup and restore]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/BackupRestore.html
[aws data pipeline]: https://aws.amazon.com/datapipeline
