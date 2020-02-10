# User Guide

This section describes how to install, configure and use the Amazon S3 Find and
Forget solution.

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

|Region|Launch Template|
|------|---------------|
|**US East (N. Virginia)** (us-east-1) | [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-us-east-1.s3.us-east-1.amazonaws.com/amazon-s3-find-and-forget/latest/main.template)|
|**US East (Ohio)** (us-east-2) | [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=us-east-2#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-us-east-2.s3.us-east-2.amazonaws.com/amazon-s3-find-and-forget/latest/main.template)|
|**US West (Oregon)** (us-west-2) | [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=us-west-2#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-us-west-2.s3.us-west-2.amazonaws.com/amazon-s3-find-and-forget/latest/main.template)|
|**Asia Pacific (Seoul)** (ap-northeast-2) | [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=ap-northeast-2#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-ap-northeast-2.s3.ap-northeast-2.amazonaws.com/amazon-s3-find-and-forget/latest/main.template)|
|**Asia Pacific (Sydney)** (ap-southeast-2) | [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=ap-southeast-2#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-ap-southeast-2.s3.ap-southeast-2.amazonaws.com/amazon-s3-find-and-forget/latest/main.template)|
|**Asia Pacific (Tokyo)** (ap-northeast-1) | [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=ap-northeast-1#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-ap-northeast-1.s3.ap-northeast-1.amazonaws.com/amazon-s3-find-and-forget/latest/main.template)|
|**EU (Ireland)** (eu-west-1) | [![Launch the Amazon S3 Find and Forget Stack with CloudFormation](./images/deploy-to-aws.png)](https://console.aws.amazon.com/cloudformation/home?region=eu-west-1#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-eu-west-1.s3.eu-west-1.amazonaws.com/amazon-s3-find-and-forget/latest/main.template)|

2. If prompted, login using your AWS account credentials.
3. You should see a screen titled "*Create Stack*" at the "*Specify template*"
   step. The fields specifying the CloudFormation template are pre-populated.
   Click the *Next* button at the bottom of the page.
4. On the "*Specify stack details*" screen you should provide values for the
   following parameters of the CloudFormation stack:
   * **Stack Name:** (Default: S3F2) This is the name that is used to refer to
   this stack in CloudFormation once deployed.
   * **AdminEmail:** The email address you wish to setup as the initial
   user of this Amazon S3 Find and Forget deployment.
   
   The following parameters are optional and allow further customisation of the
   solution if required:
   
   * **VpcIpBlock:** (Default: 10.0.0.0/16) CIDR range to use for the VPC.
   * **PrivateSubnetIpBlocks:** (Default: "10.0.0.0/22, 10.0.4.0/22, 10.0.8.0/22")
   CIDR ranges to assign to private subnets.
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
   * **WaitDurationQueryExecution:** (Default: 3) How long to wait when
   checking if an Athena Query has completed.
   * **WaitDurationQueryQueue:** (Default: 3)  How long to wait when
   checking if there the current number of executing queries is less than the
   specified concurrency limit.
   * **WaitDurationForgetQueue:** (Default: 30) How long to wait when
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
7. Click *Create Change Set* 
8. On the *Change Set* screen, click *Execute* to launch your stack.
   * You may need to wait for the *Execution status* of the change set to
   become "*AVAILABLE*" before the "*Execute*" button becomes available.
9. Wait for the CloudFormation stack to launch. Completion is indicated when
   the "Stack status" is "*CREATE_COMPLETE*".
   * You can monitor the stack creation progress in the "Events" tab.
10. Note the *WebUIUrl* displayed in the *Outputs* tab for the stack. This is
   used to access the application.

## Configuring Data Mappers
*TODO*

## Granting Access to Data
*TODO*

## Adding to the Deletion Queue
*TODO*

## Running a Deletion Job
*TODO*

## Disabling Dummy Mode
*TODO*

## Adjusting Performance Configuration
*TODO*

## Updating the Stack
*TODO*

[Fargate Configuration]: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/AWS_Fargate.html#fargate-tasks-size