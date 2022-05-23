# User Guide

This section describes how to install, configure and use the Amazon S3 Find and
Forget solution.

## Index

- [User Guide](#user-guide)
  - [Index](#index)
  - [Pre-requisites](#pre-requisites)
    - [Configuring a VPC for the Solution](#configuring-a-vpc-for-the-solution)
      - [Creating a New VPC](#creating-a-new-vpc)
      - [Using an Existing VPC](#using-an-existing-vpc)
    - [Provisioning Data Access IAM Roles](#provisioning-data-access-iam-roles)
  - [Deploying the Solution](#deploying-the-solution)
  - [Accessing the application](#accessing-the-application)
    - [Logging in for the first time (only relevant if the Web UI is deployed)](#logging-in-for-the-first-time-only-relevant-if-the-web-ui-is-deployed)
    - [Managing users (only relevant if Cognito is chosen for authentication)](#managing-users-only-relevant-if-cognito-is-chosen-for-authentication)
    - [Making authenticated API requests](#making-authenticated-api-requests)
      - [Cognito](#cognito)
      - [IAM](#iam)
    - [Integrating the solution with other applications using CloudFormation stack outputs](#integrating-the-solution-with-other-applications-using-cloudformation-stack-outputs)
  - [Configuring Data Mappers](#configuring-data-mappers)
    - [AWS Lake Formation Configuration](#aws-lake-formation-configuration)
    - [Data Mapper Creation](#data-mapper-creation)
  - [Granting Access to Data](#granting-access-to-data)
    - [Updating your Bucket Policy](#updating-your-bucket-policy)
    - [Data Encrypted with a Customer Managed CMK](#data-encrypted-with-a-customer-managed-cmk)
  - [Adding to the Deletion Queue](#adding-to-the-deletion-queue)
  - [Running a Deletion Job](#running-a-deletion-job)
    - [Deletion Job Statuses](#deletion-job-statuses)
    - [Deletion Job Event Types](#deletion-job-event-types)
  - [Adjusting Configuration](#adjusting-configuration)
  - [Updating the Solution](#updating-the-solution)
    - [Identify current solution version](#identify-current-solution-version)
    - [Identify the Stack URL to deploy](#identify-the-stack-url-to-deploy)
    - [Minor Upgrades: Perform CloudFormation Stack Update](#minor-upgrades-perform-cloudformation-stack-update)
    - [Major Upgrades: Manual Rolling Deployment](#major-upgrades-manual-rolling-deployment)
  - [Deleting the Solution](#deleting-the-solution)

## Pre-requisites

### Configuring a VPC for the Solution

The Fargate tasks used by this solution to perform deletions must be able to
access the following AWS services, either via an Internet Gateway or via [VPC
Endpoints]:

- Amazon S3 (gateway endpoint _com.amazonaws.**region**.s3_)
- Amazon DynamoDB (gateway endpoint _com.amazonaws.**region**.dynamodb_)
- Amazon CloudWatch Monitoring (interface endpoint
  _com.amazonaws.**region**.monitoring_) and Logs (interface endpoint
  _com.amazonaws.**region**.logs_)
- AWS ECR API (interface endpoint _com.amazonaws.**region**.ecr.api_) and Docker
  (interface endpoint _com.amazonaws.**region**.ecr.dkr_)
- Amazon SQS (interface endpoint _com.amazonaws.**region**.sqs_)
- AWS STS (interface endpoint _com.amazonaws.**region**.sts_)
- AWS KMS (interface endpoint _com.amazonaws.**region**.kms_) - **required only
  if S3 Objects are encrypted using AWS KMS client-side encryption**

#### Creating a New VPC

By default the CloudFormation template will create a new VPC that has been
purpose-built for the solution. The VPC includes VPC endpoints for the
aforementioned services, and does not provision internet connectivity.

You can use the provided VPC to operate the solution with no further
customisations. However, if you have more complex requirements it is recommended
to use an existing VPC as described in the following section.

#### Using an Existing VPC

Amazon S3 Find and Forget can also be used in an existing VPC. You may want to
do this if you have requirements that aren't met by using the VPC provided with
the solution.

To use an existing VPC, set the `DeployVpc` parameter to `false` when launching
the solution CloudFormation stack. You must also specify the subnet and security
groups that the Fargate tasks will use by setting the `VpcSubnets` and
`VpcSecurityGroups` parameters respectively.

The subnets and security groups that you specify must allow the tasks to connect
to the aforementioned AWS services. Forget solution, you must ensure that when
deploying the solution you select subnets and security groups which permit
access to the aforementioned services and you set _DeployVpc_ to false.

You can obtain your subnet and security group IDs from the AWS Console or by
using the AWS CLI. If using the AWS CLI, you can use the following command to
get a list of VPCs:

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

### Provisioning Data Access IAM Roles

The Fargate tasks used by this solution to perform deletions require a specific
IAM role to exist in each account that owns a bucket that you will use with the
solution. The role must have the exact name **S3F2DataAccessRole** (no path). A
CloudFormation template is available as part of this solution which can be
deployed separately to the main stack in each account. A way to deploy this role
to many accounts, for example across your organization, is to use [AWS
CloudFormation StackSets].

To deploy this template manually, use the IAM Role Template "Deploy to AWS
button" in [Deploying the Solution](#deploying-the-solution) then follow steps
5-9. The **Outputs** tab will contain the Role ARN which you will need when
adding data mappers.

You will need to grant this role read and write access to your data. We
recommend you do this using a bucket policy. For more information, see
[Granting Access to Data](#granting-access-to-data).

## Deploying the Solution

The solution is deployed as an
[AWS CloudFormation](https://aws.amazon.com/cloudformation) template and should
take about 20 to 40 minutes to deploy.

Your access to the AWS account must have IAM permissions to launch AWS
CloudFormation templates that create IAM roles and to create the solution
resources.

> **Note** You are responsible for the cost of the AWS services used while
> running this solution. For full details, see the pricing pages for each AWS
> service you will be using in this sample. Prices are subject to change.

1. Deploy the latest CloudFormation template using the AWS Console by choosing
   the "_Launch Template_" button below for your preferred AWS region. If you
   wish to [deploy using the AWS CLI] instead, you can refer to the "_Template
   Link_" to download the template files.

| Region                                     | Launch Template                                                                                                                                                                                                                                   | Template Link                                                                                                                   | Launch IAM Role Template                                                                                                                                                                                                                           | IAM Role Template Link                                                                                                      |
| ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| **US East (N. Virginia)** (us-east-1)      | [Launch](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-us-east-1.s3.us-east-1.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml)                | [Link](https://solution-builders-us-east-1.s3.us-east-1.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml)           | [Launch](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/new?stackName=S3F2-Role&templateURL=https://solution-builders-us-east-1.s3.us-east-1.amazonaws.com/amazon-s3-find-and-forget/latest/role.yaml)                | [Link](https://solution-builders-us-east-1.s3.us-east-1.amazonaws.com/amazon-s3-find-and-forget/latest/role.yaml)           |
| **US East (Ohio)** (us-east-2)             | [Launch](https://console.aws.amazon.com/cloudformation/home?region=us-east-2#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-us-east-2.s3.us-east-2.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml)                | [Link](https://solution-builders-us-east-2.s3.us-east-2.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml)           | [Launch](https://console.aws.amazon.com/cloudformation/home?region=us-east-2#/stacks/new?stackName=S3F2-Role&templateURL=https://solution-builders-us-east-2.s3.us-east-2.amazonaws.com/amazon-s3-find-and-forget/latest/role.yaml)                | [Link](https://solution-builders-us-east-2.s3.us-east-2.amazonaws.com/amazon-s3-find-and-forget/latest/role.yaml)           |
| **US West (Oregon)** (us-west-2)           | [Launch](https://console.aws.amazon.com/cloudformation/home?region=us-west-2#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-us-west-2.s3.us-west-2.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml)                | [Link](https://solution-builders-us-west-2.s3.us-west-2.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml)           | [Launch](https://console.aws.amazon.com/cloudformation/home?region=us-west-2#/stacks/new?stackName=S3F2-Role&templateURL=https://solution-builders-us-west-2.s3.us-west-2.amazonaws.com/amazon-s3-find-and-forget/latest/role.yaml)                | [Link](https://solution-builders-us-west-2.s3.us-west-2.amazonaws.com/amazon-s3-find-and-forget/latest/role.yaml)           |
| **Asia Pacific (Sydney)** (ap-southeast-2) | [Launch](https://console.aws.amazon.com/cloudformation/home?region=ap-southeast-2#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-ap-southeast-2.s3.ap-southeast-2.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml) | [Link](https://solution-builders-ap-southeast-2.s3.ap-southeast-2.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml) | [Launch](https://console.aws.amazon.com/cloudformation/home?region=ap-southeast-2#/stacks/new?stackName=S3F2-Role&templateURL=https://solution-builders-ap-southeast-2.s3.ap-southeast-2.amazonaws.com/amazon-s3-find-and-forget/latest/role.yaml) | [Link](https://solution-builders-ap-southeast-2.s3.ap-southeast-2.amazonaws.com/amazon-s3-find-and-forget/latest/role.yaml) |
| **Asia Pacific (Tokyo)** (ap-northeast-1)  | [Launch](https://console.aws.amazon.com/cloudformation/home?region=ap-northeast-1#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-ap-northeast-1.s3.ap-northeast-1.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml) | [Link](https://solution-builders-ap-northeast-1.s3.ap-northeast-1.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml) | [Launch](https://console.aws.amazon.com/cloudformation/home?region=ap-northeast-1#/stacks/new?stackName=S3F2-Role&templateURL=https://solution-builders-ap-northeast-1.s3.ap-northeast-1.amazonaws.com/amazon-s3-find-and-forget/latest/role.yaml) | [Link](https://solution-builders-ap-northeast-1.s3.ap-northeast-1.amazonaws.com/amazon-s3-find-and-forget/latest/role.yaml) |
| **EU (Ireland)** (eu-west-1)               | [Launch](https://console.aws.amazon.com/cloudformation/home?region=eu-west-1#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-eu-west-1.s3.eu-west-1.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml)                | [Link](https://solution-builders-eu-west-1.s3.eu-west-1.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml)           | [Launch](https://console.aws.amazon.com/cloudformation/home?region=eu-west-1#/stacks/new?stackName=S3F2-Role&templateURL=https://solution-builders-eu-west-1.s3.eu-west-1.amazonaws.com/amazon-s3-find-and-forget/latest/role.yaml)                | [Link](https://solution-builders-eu-west-1.s3.eu-west-1.amazonaws.com/amazon-s3-find-and-forget/latest/role.yaml)           |
| **EU (London)** (eu-west-2)                | [Launch](https://console.aws.amazon.com/cloudformation/home?region=eu-west-2#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-eu-west-2.s3.eu-west-2.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml)                | [Link](https://solution-builders-eu-west-2.s3.eu-west-2.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml)           | [Launch](https://console.aws.amazon.com/cloudformation/home?region=eu-west-2#/stacks/new?stackName=S3F2-Role&templateURL=https://solution-builders-eu-west-2.s3.eu-west-2.amazonaws.com/amazon-s3-find-and-forget/latest/role.yaml)                | [Link](https://solution-builders-eu-west-2.s3.eu-west-2.amazonaws.com/amazon-s3-find-and-forget/latest/role.yaml)           |
| **EU (Frankfurt)** (eu-central-1)          | [Launch](https://console.aws.amazon.com/cloudformation/home?region=eu-central-1#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-eu-central-1.s3.eu-central-1.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml)       | [Link](https://solution-builders-eu-central-1.s3.eu-central-1.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml)     | [Launch](https://console.aws.amazon.com/cloudformation/home?region=eu-central-1#/stacks/new?stackName=S3F2-Role&templateURL=https://solution-builders-eu-central-1.s3.eu-central-1.amazonaws.com/amazon-s3-find-and-forget/latest/role.yaml)       | [Link](https://solution-builders-eu-central-1.s3.eu-central-1.amazonaws.com/amazon-s3-find-and-forget/latest/role.yaml)     |
| **EU (Stockholm)** (eu-north-1)            | [Launch](https://console.aws.amazon.com/cloudformation/home?region=eu-north-1#/stacks/new?stackName=S3F2&templateURL=https://solution-builders-eu-north-1.s3.eu-north-1.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml)             | [Link](https://solution-builders-eu-north-1.s3.eu-north-1.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml)         | [Launch](https://console.aws.amazon.com/cloudformation/home?region=eu-north-1#/stacks/new?stackName=S3F2-Role&templateURL=https://solution-builders-eu-north-1.s3.eu-north-1.amazonaws.com/amazon-s3-find-and-forget/latest/role.yaml)             | [Link](https://solution-builders-eu-north-1.s3.eu-north-1.amazonaws.com/amazon-s3-find-and-forget/latest/role.yaml)         |

2. If prompted, login using your AWS account credentials.
3. You should see a screen titled "_Create Stack_" at the "_Specify template_"
   step. The fields specifying the CloudFormation template are pre-populated.
   Choose the _Next_ button at the bottom of the page.
4. On the "_Specify stack details_" screen you should provide values for the
   following parameters of the CloudFormation stack:

   - **Stack Name:** (Default: S3F2) This is the name that is used to refer to
     this stack in CloudFormation once deployed.
   - **AdminEmail:** The email address you wish to setup as the initial user of
     this Amazon S3 Find and Forget deployment.
   - **DeployWebUI:** (Default: true) Whether to deploy the Web UI as part of
     the solution. If set to **true**, the AuthMethod parameter must be set to
     **Cognito**. If set to **false**, interaction with the solution is
     performed via the API Gateway only.

     _NOTE_: Changing the DeployWebUI parameter is not supported. If you wish
     change this parameter it must be done by deploying a new CloudFormation
     stack.

   - **AuthMethod:** (Default: Cognito) The authentication method to be used for
     the solution. Must be set to **Cognito** if DeployWebUI is true.

     _NOTE_: Changing the AuthMethod parameter is supported. A manual deployment
     (through the API Gateway console) of the updated API Gateway Stage is
     required once the Stack update is complete.

   The following parameters are optional and allow further customisation of the
   solution if required:

   - **DeployVpc:** (Default: true) Whether to deploy the solution provided VPC.
     If you wish to use your own VPC, set this value to false. The solution
     provided VPC uses VPC Endpoints to access the required services which will
     incur additional costs. For more details, see the [VPC Endpoint Pricing]
     page.
   - **VpcSecurityGroups:** (Default: "") List of security group IDs to apply to
     Fargate deletion tasks. For more information on how to obtain these IDs,
     see
     [Configuring a VPC for the Solution](#configuring-a-vpc-for-the-solution).
     If _DeployVpc_ is true, this parameter is ignored.
   - **VpcSubnets:** (Default: "") List of subnets to run Fargate deletion tasks
     in. For more information on how to obtain these IDs, see
     [Configuring a VPC for the Solution](#configuring-a-vpc-for-the-solution).
     If _DeployVpc_ is true, this parameter is ignored.
   - **FlowLogsGroup**: (Default: "") If using the solution provided VPC,
     defines the CloudWatch Log group which should be used for flow logs. If not
     set, flow logs will not be enabled. If _DeployVpc_ is false, this parameter
     is ignored. Enabling flow logs will incur additional costs. See the
     [CloudWatch Logs Pricing] page for the associated costs.
   - **FlowLogsRoleArn**: (Default: "") If using the solution provided VPC,
     defines which IAM Role should be used to send flow logs to CloudWatch. If
     not set, flow logs will not be enabled. If _DeployVpc_ is false, this
     parameter is ignored.
   - **CreateCloudFrontDistribution:** (Default: true) Creates a CloudFront
     distribution for accessing the web interface of the solution.
   - **AccessControlAllowOriginOverride:** (Default: false) Allows overriding
     the origin from which the API can be called. If 'false' is provided, the
     API will only accept requests from the Web UI origin.
   - **AthenaConcurrencyLimit:** (Default: 20) The number of concurrent Athena
     queries the solution will run when scanning your data lake.
   - **AthenaQueryMaxRetries:** (Default: 2) Max number of retries to each
     Athena query after a failure
   - **DeletionTasksMaxNumber:** (Default: 3) Max number of concurrent Fargate
     tasks to run when performing deletions.
   - **DeletionTaskCPU:** (Default: 4096) Fargate task CPU limit. For more info
     see [Fargate Configuration]
   - **DeletionTaskMemory:** (Default: 30720) Fargate task memory limit. For
     more info see [Fargate Configuration]
   - **QueryExecutionWaitSeconds:** (Default: 3) How long to wait when checking
     if an Athena Query has completed.
   - **QueryQueueWaitSeconds:** (Default: 3) How long to wait when checking if
     there the current number of executing queries is less than the specified
     concurrency limit.
   - **ForgetQueueWaitSeconds:** (Default: 30) How long to wait when checking if
     the Forget phase is complete
   - **AccessLogsBucket:** (Default: "") The name of the bucket to use for
     storing the Web UI access logs. Leave blank to disable UI access logging.
     Ensure the provided bucket has the appropriate permissions configured. For
     more information see [CloudFront Access Logging Permissions] if
     **CreateCloudFrontDistribution** is set to true, or [S3 Access Logging
     Permissions] if not.
   - **CognitoAdvancedSecurity:** (Default: "OFF") The setting to use for
     Cognito advanced security. Allowed values for this parameter are: OFF,
     AUDIT and ENFORCED. For more information on this parameter, see [Cognito
     Advanced Security]
   - **EnableAPIAccessLogging:** (Default: false) Whether to enable access
     logging via CloudWatch Logs for API Gateway. Enabling this feature will
     incur additional costs.
   - **EnableContainerInsights:** (Default: false) Whether to enable CloudWatch
     Container Insights.
   - **JobDetailsRetentionDays:** (Default: 0) How long job records should
     remain in the Job table and how long job manifests should remain in the S3
     manifests bucket. Use 0 to retain data indefinitely. **Note**: if the
     retention setting is changed it will only apply to new deletion jobs in
     DynamoDB, existing deletion jobs will retain the TTL at the time they were
     ran; but the policy will apply immediately to new and existing job
     manifests in S3.
   - **EnableDynamoDBBackups:** (Default: false) Whether to enable [DynamoDB
     Point-in-Time Recovery] for the DynamoDB tables. Enabling this feature will
     incur additional costs. See the [DynamoDB Pricing] page for the associated
     costs.
   - **RetainDynamoDBTables:** (Default: true) Whether to retain the DynamoDB
     tables upon Stack Update and Stack Deletion.
   - **AthenaWorkGroup:** (Default: primary) The Athena work group that should
     be used for when the solution runs Athena queries.
   - **PreBuiltArtefactsBucketOverride:** (Default: false) Overrides the default
     Bucket containing Front-end and Back-end pre-built artefacts. Use this if
     you are using a customised version of these artefacts.
   - **ResourcePrefix:** (Default: S3F2) Resource prefix to apply to resource
     names when creating statically named resources.
   - **KMSKeyArns** (Default: "") Comma-delimited list of KMS Key Arns used for
     Client-side Encryption. Leave empty if data is not client-side encrypted
     with KMS.

   When completed, click _Next_

5. [Configure stack options](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/cfn-console-add-tags.html)
   if desired, then click _Next_.
6. On the review screen, you must check the boxes for:

   - "_I acknowledge that AWS CloudFormation might create IAM resources_"
   - "_I acknowledge that AWS CloudFormation might create IAM resources with
     custom names_"
   - "_I acknowledge that AWS CloudFormation might require the following
     capability: CAPABILITY_AUTO_EXPAND_"

   These are required to allow CloudFormation to create a Role to allow access
   to resources needed by the stack and name the resources in a dynamic way.

7. Choose _Create Stack_
8. Wait for the CloudFormation stack to launch. Completion is indicated when the
   "Stack status" is "_CREATE_COMPLETE_".
   - You can monitor the stack creation progress in the "Events" tab.
9. Note the _WebUIUrl_ displayed in the _Outputs_ tab for the stack. This is
   used to access the application.

## Accessing the application

The solution provides a web user interface and a REST API to allow you to
integrate it in your own applications. If you have chosen not to deploy the Web
UI you will need to use the API to interface with the solution.

### Logging in for the first time (only relevant if the Web UI is deployed)

1. Note the _WebUIUrl_ displayed in the _Outputs_ tab for the stack. This is
   used to access the application.
2. When accessing the web user interface for the first time, you will be
   prompted to insert a username and a password. In the username field, enter
   the admin e-mail specified during stack creation. In the password field,
   enter the temporary password sent by the system to the admin e-mail. Then
   select "Sign In".
3. Next, you will need to reset the password. Enter a new password and then
   select "Submit".
4. Now you should be able to access all the functionalities.

### Managing users (only relevant if Cognito is chosen for authentication)

To add more users to the application:

1. Access the [Cognito Console] and choose "Manage User Pools".
2. Select the solution's User Pool (its name is displayed as
   _CognitoUserPoolName_ in the _Outputs_ tab for the CloudFormation stack).
3. Select "Users and Groups" from the menu on the right.
4. Use this page to create or manage users. For more information, consult the
   [Managing Users in User Pools Guide].

### Making authenticated API requests

To use the API directly, you will need to authenticate requests using the
Cognito User Pool or IAM. The method for authenticating differs depending on
which authentication option was chosen:

#### Cognito

After resetting the password via the UI, you can make authenticated requests
using the AWS CLI:

1. Note the _CognitoUserPoolId_, _CognitoUserPoolClientId_ and _ApiUrl_
   parameters displayed in the _Outputs_ tab for the stack.
2. Take note of the Cognito user email and password.
3. Generate a token by running this command with the values you noted in the
   previous steps:

   ```sh
   aws cognito-idp admin-initiate-auth \
     --user-pool-id $COGNITO_USER_POOL_ID \
     --client-id $COGNITO_USER_POOL_CLIENT_ID \
     --auth-flow ADMIN_NO_SRP_AUTH \
     --auth-parameters '{"USERNAME":"$USER_EMAIL_ADDRESS","PASSWORD":"$USER_PASSWORD"}'
   ```

4. Use the `IdToken` generated by the previous command to make an authenticated
   request to the API. For instance, the following command will show the matches
   in the deletion queue:

   ```sh
   curl $API_URL/v1/queue -H "Authorization: Bearer $ID_TOKEN"
   ```

#### IAM

IAM authentication for API requests uses the
[Signature Version 4 signing process](https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html).
Add the resulting signature to the **Authorization** header when making requests
to the API.

Use the Sigv4 process linked above to generate the Authorization header value
and then call the API as normal:

```sh
curl $API_URL/v1/queue -H "Authorization: $Sigv4Auth"
```

For more information, consult the [Cognito REST API integration guide].

### Integrating the solution with other applications using CloudFormation stack outputs

Applications deployed using AWS CloudFormation in the same AWS account and
region can integrate with Find and Forget by using CloudFormation output values.
You can use the solution stack as a nested stack to use its outputs (such as the
API URL) as inputs for another application.

Some outputs are also available as exports. You can import these values to use
in your own CloudFormation stacks that you deploy following the Find and Forget
stack.

**Note for using exports:** After another stack imports an output value, you
can't delete the stack that is exporting the output value or modify the exported
output value. All of the imports must be removed before you can delete the
exporting stack or modify the output value.

Consult the [exporting stack output values] guide to review the differences
between importing exported values and using nested stacks.

## Configuring Data Mappers

After [Deploying the Solution](#deploying-the-solution), your first step should
be to configure one or more [data mappers](ARCHITECTURE.md#data-mappers) which
will connect your data to the solution. Identify the S3 Bucket containing the
data you wish to connect to the solution and ensure you have defined a table in
your data catalog and that all existing and future partitions (as they are
created) are known to the Data Catalog. Currently AWS Glue is the only supported
data catalog provider. For more information on defining your data in the Glue
Data Catalog, see [Defining Glue Tables]. You must define your Table in the Glue
Data Catalog in the same region and account as the S3 Find and Forget solution.

### AWS Lake Formation Configuration

For data lakes registered with AWS Lake Formation, you must grant additional
permissions in Lake Formation before you can use them with the solution. If you
are not using Lake Formation, proceed directly to the
[Data Mapper creation](#data-mapper-creation) section.

To grant these permissions in Lake Formation:

1. Using the **WebUIRole** output from the solution CloudFormation stack as the
   IAM principal, use the [Lake Formation Data Permissions Console] to grant the
   `Describe` permission for all Glue Databases that you will want to use with
   the solution; then grant the `Describe` and `Select` permissions to the role
   for all Glue Tables that you will want to use with the solution. These
   permissions are necessary to create data mappers in the web interface.
2. Using the **PutDataMapperRole** output from the solution CloudFormation stack
   as the IAM principal, use the [Lake Formation Data Permissions Console] to
   grant `Describe` and `Select` permissions for all Glue Tables that you will
   want to use with the solution. These permissions allow the solution to access
   Table metadata when creating a Data Mapper.
3. Using the **AthenaExecutionRole** and **GenerateQueriesRole** outputs from
   the solution CloudFormation stack as IAM principals, use the [Lake Formation
   Data Permissions Console] to grant the `Describe` and `Select` permissions to
   both principals for all of the tables that you will want to use with the
   solution. These permissions allow the solution to plan and execute Athena
   queries during the Find Phase.

### Data Mapper Creation

1. Access the application UI via the **WebUIUrl** displayed in the _Outputs_ tab
   for the stack.
2. Choose **Data Mappers** from the menu then choose **Create Data Mapper**
3. On the Create Data Mapper page input a **Name** to uniquely identify this
   Data Mapper.
4. Select a **Query Executor Type** then choose the **Database** and **Table**
   in your data catalog which describes the target data in S3. A list of columns
   will be displayed for the chosen Table.
5. From the Partition Keys list, select the partition key(s) that you want the
   solution to use when generating the queries. If you select none, only one
   query will be performed for the data mapper. If you select any or all, you'll
   have a greater number of smaller queries (the same query will be repeated
   with a `WHERE` additional clause for each combination of partition values).
   If you have a lot of small partitions, it may be more efficient to choose
   none or a subset of partition keys from the list in order to increase speed
   of execution. If instead you have very big partitions, it may be more
   efficient to choose all the partition keys in order to reduce probability of
   failure caused by query timeout. We recommend the average query size not to
   exceed the hundreds of GBs and not to take more than 5 minutes.

   > As an example, let's consider 10 years of daily data with partition keys of
   > `year`, `month` and `day` with total size of `10TB`. By declaring
   > PartitionKeys=`[]` (none) a single query of `10TB` would run during the
   > Find phase, and that may be too much to complete within the 30m limit of
   > Athena execution time. On the other hand, using all the combinations of the
   > partition keys we would have approximately `3652` queries, each being
   > probably very small, and given the default Athena concurrency limit of
   > `20`, it may take very long to execute all of them. The best in this
   > scenario is possibly the `['year','month']` combination, which would result
   > in `120` queries.

6. From the columns list, choose the column(s) the solution should use to to
   find items in the data which should be deleted. For example, if your table
   has three columns named **customer_id**, **description** and **created_at**
   and you want to search for items using the **customer_id**, you should choose
   only the **customer_id** column from this list.
7. Enter the ARN of the role for Fargate to assume when modifying objects in S3
   buckets. This role should already exist if you have followed the
   [Provisioning Data Access IAM Roles](#provisioning-data-access-iam-roles)
   steps.
8. If you do not want the solution to delete all older versions except the
   latest created object version, deselect _Delete previous object versions
   after update_. By default the solution will delete all previous of versions
   after creating a new version.
9. If you want the solution to ignore Object Not Found exceptions, select
   _Ignore object not found exceptions during deletion_. By default deletion
   jobs will fail if any objects that are found by the Find phase don't exist in
   the Delete phase. This setting can be useful if you have some other system
   deleting objects from the bucket, for example S3 lifecycle policies.

   Note that the solution **will not** delete old versions for these objects.
   This can cause data to be **retained longer than intended**. Make sure there
   is some mechanism to handle old versions. One option would be to configure
   [S3 lifecycle policies] on non-current versions.

10. Choose **Create Data Mapper**.
11. A message is displayed advising you to update the S3 Bucket Policy for the
    S3 Bucket referenced by the newly created data mapper. See
    [Granting Access to Data](#granting-access-to-data) for more information on
    how to do this. Choose **Return to Data Mappers**.

You can also create Data Mappers directly via the API. For more information, see
the [API Documentation].

## Granting Access to Data

After configuring a data mapper you must ensure that the S3 Find and Forget
solution has the required level of access to the S3 location the data mapper
refers to. The recommended way to achieve this is through the use of [S3 Bucket
Policies].

> **Note:** AWS IAM uses an
> [eventual consistency model](https://docs.aws.amazon.com/IAM/latest/UserGuide/troubleshoot_general.html#troubleshoot_general_eventual-consistency)
> and therefore any change you make to IAM, Bucket or KMS Key policies may take
> time to become visible. Ensure you have allowed time for permissions changes
> to propagate to all endpoints before starting a job. If your job fails with a
> status of FIND_FAILED and the `QueryFailed` events indicate S3 permissions
> issues, you may need to wait for the permissions changes to propagate.

### Updating your Bucket Policy

To update the S3 bucket policy to grant **read** access to the IAM role used by
Amazon Athena, and **write** access to the Data Access IAM role used by AWS
Fargate, follow these steps:

1. Access the application UI via the **WebUIUrl** displayed in the _Outputs_ tab
   for the stack.
2. Choose **Data Mappers** from the menu then choose the radio button for the
   relevant data mapper from the **Data Mappers** list.
3. Choose **Generate Access Policies** and follow the instructions on the
   **Bucket Access** tab to update the bucket policy. If you already have a
   bucket policy in place, add the statements shown to your existing bucket
   policy rather than replacing it completely. If your data is encrypted with an
   **Customer Managed CMK** rather than an **AWS Managed CMK**, see
   [Data Encrypted with Customer Managed CMK](#data-encrypted-with-a-customer-managed-cmk)
   to grant the solution access to the Customer Managed CMK. For more
   information on using Server-Side Encryption (SSE) with S3, see [Using SSE
   with CMKs].

### Data Encrypted with a Customer Managed CMK

Where the data you are connecting to the solution is encrypted with an Customer
Managed CMK rather than an AWS Managed CMK, you must also grant the Athena and
Data Access IAM roles access to use the key so that the data can be decrypted
when reading, re-encrypted when writing.

Once you have updated the bucket policy as described in
[Updating the Bucket Policy](#updating-the-bucket-policy), choose the **KMS
Access** tab from the **Generate Access Policies** modal window and follow the
instructions to update the key policy with the provided statements. The
statements provided are for use when using the **policy view** in the AWS
console or making updates to the key policy via the CLI, CloudFormation or the
API. If you wish, to use the **default view** in th AWS console, add the
**Principals** in the provided statements as **key users**. For more
information, see [How to Change a Key Policy].

## Adding to the Deletion Queue

Once your Data Mappers are configured, you can begin adding "Matches" to the
[Deletion Queue](ARCHITECTURE.md#deletion-queue).

1. Access the application UI via the **WebUIUrl** displayed in the _Outputs_ tab
   for the stack.
2. Choose **Deletion Queue** from the menu then choose **Add Match to the
   Deletion Queue**.

Matches can be **Simple** or **Composite**.

- A **Simple** match is a value to be matched against any column identifier of
  one or more data mappers. For instance a value _12345_ to be matched against
  the _customer_id_ column of _DataMapperA_ or the _admin_id_ of _DataMapperB_.
- A **Composite** match consists on one or more values to be matched against
  specific column identifiers of a multi-column based data mapper. For instance
  a tuple _John_ and _Doe_ to be matched against the _first_name_ and
  _last_name_ columns of _DataMapperC_

To add a simple match:

1. Choose _Simple_ as **Match Type**
2. Input a **Match**, which is the value to search for in your data mappers. If
   you wish to search for the match from all data mappers choose **All Data
   Mappers**, otherwise choose **Select your Data Mappers** then select the
   relevant data mappers from the list.
3. Choose **Add Item to the Deletion Queue** and confirm you can see the match
   in the Deletion Queue.

To add a composite match you need to have at least one data mapper with more
than one column identifier. Then:

1. Choose _Composite_ as **Match Type**
2. Select the Data Mapper from the List
3. Select all the columns (at least one) that you want to map to a match and
   then provide a value for each of them. Empty is a valid value.
4. Choose **Add Item to the Deletion Queue** and confirm you can see the match
   in the Deletion Queue.

You can also add matches to the Deletion Queue directly via the API. For more
information, see the [API Documentation].

When the next deletion job runs, the solution will scan the configured columns
of your data for any occurrences of the Matches present in the queue at the time
the job starts and remove any items where one of the Matches is present.

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
When the job executes, it will merge the lists of data mappers for duplicates in
the queue.

## Running a Deletion Job

Once you have configured your data mappers and added one or more items to the
deletion queue, you can stat a job.

1. Access the application UI via the **WebUIUrl** displayed in the _Outputs_ tab
   for the stack.
2. Choose **Deletion Jobs** from the menu and ensure there are no jobs currently
   running. Choose **Start a Deletion Job** and review the settings displayed on
   the screen. For more information on how to edit these settings, see
   [Adjusting Configuration](#adjusting-configuration).
3. If you are happy with the current solution configuration choose **Start a
   Deletion Job**. The job details page should be displayed.

Once a job has started, you can leave the page and return to view its progress
at point by choosing the job ID from the Deletion Jobs list. The job details
page will automatically refresh and to display the current status and statistics
for the job. For more information on the possible statuses and their meaning,
see [Deletion Job Statuses](#deletion-job-statuses).

You can also start jobs and check their status using the API. For more
information, see the [API Documentation].

Job events are continuously emitted whilst a job is running. These events are
used to update the status and statistics for the job. You can view all the
emitted events for a job in the **Job Events** table. Whilst a job is running,
the **Load More** button will continue to be displayed even if no new events
have been received. Once a job has finished, the **Load More** button will
disappear once you have loaded all the emitted events. For more information on
the events which can be emitted during a job, see
[Deletion Job Event Types](#deletion-job-event-types)

To optimise costs, it is best practice when using the solution to start jobs on
a regular schedule, rather than every time a single item is added to the
Deletion Queue. This is because the marginal cost of the Find phase when
deleting an additional item from the queue is far less that re-executing the
Find phase (where the data mappers searched are the same). Similarly, the
marginal cost of removing an additional match from an object is negligible when
there is already at least 1 match present in the object contents.

> **Important**
>
> Ensure no external processes perform write/delete actions against exist
> objects whilst a job is running. For more information, consult the [Limits]
> guide

### Deletion Job Statuses

The list of possible job statuses is as follows:

- `QUEUED`: The job has been accepted but has yet to start. Jobs are started
  asynchronously by a Lambda invoked by the [DynamoDB event
  stream][dynamodb streams] for the Jobs table.
- `RUNNING`: The job is still in progress.
- `FORGET_COMPLETED_CLEANUP_IN_PROGRESS`: The job is still in progress.
- `COMPLETED`: The job finished successfully.
- `COMPLETED_CLEANUP_FAILED`: The job finished successfully however the deletion
  queue items could not be removed. You should manually remove these or leave
  them to be removed on the next job
- `FORGET_PARTIALLY_FAILED`: The job finished but it was unable to successfully
  process one or more objects. The Deletion DLQ for messages will contain a
  message per object that could not be updated.
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

- `JobStarted`: Emitted when the deletion job state machine first starts. Causes
  the status of the job to transition from `QUEUED` to `RUNNING`
- `FindPhaseStarted`: Emitted when the deletion job has purged any messages from
  the query and object queues and is ready to be searching for data.
- `FindPhaseEnded`: Emitted when all queries have executed and written their
  results to the objects queue.
- `FindPhaseFailed`: Emitted when one or more queries fail. Causes the status to
  transition to `FIND_FAILED`.
- `ForgetPhaseStarted`: Emitted when the Find phase has completed successfully
  and the Forget phase is starting.
- `ForgetPhaseEnded`: Emitted when the Forget phase has completed. If the Forget
  phase completes with no errors, this event causes the status to transition to
  `FORGET_COMPLETED_CLEANUP_IN_PROGRESS`. If the Forget phase completes but
  there was an error updating one or more objects, this causes the status to
  transition to `FORGET_PARTIALLY_FAILED`.
- `ForgetPhaseFailed`: Emitted when there was an issue running the Fargate
  tasks. Causes the status to transition to `FORGET_FAILED`.
- `CleanupSucceeded`: The **final** event emitted when a job has executed
  successfully and the Deletion Queue has been cleaned up. Causes the status to
  transition to `COMPLETED`.
- `CleanupFailed`: The **final** event emitted when the job executed
  successfully but there was an error removing the processed matches from the
  Deletion Queue. Causes the status to transition to `COMPLETED_CLEANUP_FAILED`.
- `CleanupSkipped`: Emitted when the job is finalising and the job status is one
  of `FIND_FAILED`, `FORGET_FAILED` or `FAILED`.
- `QuerySucceeded`: Emitted whenever a single query executes successfully.
- `QueryFailed`: Emitted whenever a single query fails.
- `ObjectUpdated`: Emitted whenever an updated object is written to S3 and any
  associated deletions are complete.
- `ObjectUpdateFailed`: Emitted whenever an object cannot be updated, an object
  version integrity conflict is detected or an associated deletion fails.
- `ObjectRollbackFailed`: Emitted whenever a rollback (triggered by a detected
  version integrity conflict) fails.
- `Exception`: Emitted whenever a generic error occurs during the job execution.
  Causes the status to transition to `FAILED`.

## Adjusting Configuration

There are several parameters to set when
[Deploying the Solution](#deploying-the-solution) which affect the behaviour of
the solution in terms of data retention and performance:

- `AthenaConcurrencyLimit`: Increasing the number of concurrent queries that
  should be executed will decrease the total time spent performing the Find
  phase. You should not increase this value beyond your account Service Quota
  for concurrent DML queries, and should ensure that the value set takes into
  account any other Athena DML queries that may be executing whilst a job is
  running.
- `DeletionTasksMaxNumber`: Increasing the number of concurrent tasks that
  should consume messages from the object queue will decrease the total time
  spent performing the Forget phase.
- `QueryExecutionWaitSeconds`: Decreasing this value will decrease the length of
  time between each check to see whether a query has completed. You should aim
  to set this to the "ceiling function" of your average query time. For example,
  if you average query takes 3.2 seconds, set this to 4.
- `QueryQueueWaitSeconds`: Decreasing this value will decrease the length of
  time between each check to see whether additional queries can be scheduled
  during the Find phase. If your jobs fail due to exceeding the Step Functions
  execution history quota, you may have set this value to low and should
  increase it to allow more queries to be scheduled after each check.
- `ForgetQueueWaitSeconds`: Decreasing this value will decrease the length of
  time between each check to see whether the Fargate object queue is empty. If
  your jobs fail due to exceeding the Step Functions execution history quota,
  you may have set this value to low.
- `JobDetailsRetentionDays`: Changing this value will change how long records
  job details and events are retained for. Set this to 0 to retain them
  indefinitely.

The values for these parameters are stored in an SSM Parameter Store String
Parameter named `/s3f2/S3F2-Configuration` as a JSON object. The recommended
approach for updating these values is to perform a
[Stack Update](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-updating-stacks-direct.html)
and change the relevant parameters for the stack.

It is possible to [update the SSM Parameter][updating an ssm parameter] directly
however this is not a recommended approach. **You should not alter the structure
or data types of the configuration JSON object.**

Once updated, the configuration will affect any **future** job executions. In
progress and previous executions will **not** be affected. The current
configuration values are displayed when confirming that you wish to start a job.

You can only update the vCPUs/memory allocated to Fargate tasks by performing a
stack update. For more information, see
[Updating the Solution](#updating-the-solution).

## Updating the Solution

To benefit from the latest features and improvements, you should update the
solution deployed to your account when a new version is published. To find out
what the latest version is and what has changed since your currently deployed
version, check the [Changelog].

How you update the solution depends on the difference between versions. If the
new version is a _minor_ upgrade (for instance, from version 3.45 to 3.67) you
should deploy using a CloudFormation Stack Update. If the new version is a
_major_ upgrade (for instance, from 2.34 to 3.0) you should perform a manual
rolling deployment.

Major version releases are made in exceptional circumstances and may contain
changes that prohibit backward compatibility. Minor versions releases are
backward-compatible.

### Identify current solution version

You can find the version of the currently deployed solution by retrieving the
`SolutionVersion` output for the solution stack. The solution version is also
shown on the Dashboard of the Web UI.

### Identify the Stack URL to deploy

After reviewing the [Changelog], obtain the `Template Link` url of the latest
version from ["Deploying the Solution"](#deploying-the-solution) (it will be
similar to
`https://solution-builders-us-east-1.s3.us-east-1.amazonaws.com/amazon-s3-find-and-forget/latest/template.yaml`).
If you wish to deploy a specific version rather than the latest version, replace
`latest` from the url with the chosen version, for instance
`https://solution-builders-us-east-1.s3.us-east-1.amazonaws.com/amazon-s3-find-and-forget/v0.2/template.yaml`.

### Minor Upgrades: Perform CloudFormation Stack Update

To deploy via AWS Console:

1. Open the [CloudFormation Console Page] and choose the Solution by selecting
   to the stack's radio button, then choose "Update"
2. Choose "Replace current template" and then input the template URL for the
   version you wish to deploy in the "Amazon S3 URL" textbox, then choose "Next"
3. On the _Stack Details_ screen, review the Parameters and then choose "Next"
4. On the _Configure stack options_ screen, choose "Next"
5. On the _Review stack_ screen, you must check the boxes for:

   - "_I acknowledge that AWS CloudFormation might create IAM resources_"
   - "_I acknowledge that AWS CloudFormation might create IAM resources with
     custom names_"
   - "_I acknowledge that AWS CloudFormation might require the following
     capability: CAPABILITY_AUTO_EXPAND_"

   These are required to allow CloudFormation to create a Role to allow access
   to resources needed by the stack and name the resources in a dynamic way.

6. Choose "Update stack" to start the stack update.
7. Wait for the CloudFormation stack to finish updating. Completion is indicated
   when the "Stack status" is "_UPDATE_COMPLETE_".

To deploy via the AWS CLI
[consult the documentation](https://docs.aws.amazon.com/cli/latest/reference/cloudformation/update-stack.html).

### Major Upgrades: Manual Rolling Deployment

The process for a manual rolling deployment is as follows:

1. Create a new stack from scratch
2. Export the data from the old stack to the new stack
3. Migrate consumers to new API and Web UI URLs
4. Delete the old stack.

The steps for performing this process are:

1. Deploy a new instance of the Solution by following the instructions contained
   in the ["Deploying the Solution" section](#deploying-the-solution). Make sure
   you use unique values for Stack Name and ResourcePrefix parameter which
   differ from existing stack.
2. Migrate Data from DynamoDB to ensure the new stack contains the necessary
   configuration related to Data Mappers and settings. When both stacks are
   deployed in the same account and region, the simplest way to migrate is via
   [On-Demand Backup and Restore]. If the stacks are deployed in different
   regions or accounts, you can use [AWS Data Pipeline].
3. Ensure that all the bucket policies for the Data Mappers are in place for the
   new stack. See the
   ["Granting Access to Data" section](#granting-access-to-data) for steps to do
   this.
4. Review the [Changelog] for changes that may affect how you use the new
   deployment. This may require you to make changes to any software you have
   that interacts with the solution's API.
5. Once all the consumers are migrated to the new stack (API and Web UI), delete
   the old stack.

## Deleting the Solution

To delete a stack via AWS Console:

1. Open the [CloudFormation Console Page] and choose the solution stack, then
   choose "Delete"
2. Once the confirmation modal appears, choose "Delete stack".
3. Wait for the CloudFormation stack to finish updating. Completion is indicated
   when the "Stack status" is "_DELETE_COMPLETE_".

To delete a stack via the AWS CLI
[consult the documentation](https://docs.aws.amazon.com/cli/latest/reference/cloudformation/delete-stack.html).

[api documentation]: api/README.md
[troubleshooting]: TROUBLESHOOTING.md
[fargate configuration]:
  https://docs.aws.amazon.com/AmazonECS/latest/developerguide/AWS_Fargate.html#fargate-tasks-size
[vpc endpoints]:
  https://docs.aws.amazon.com/vpc/latest/userguide/vpc-endpoints.html
[vpc endpoint pricing]: https://aws.amazon.com/privatelink/pricing/
[cloudwatch logs pricing]: https://aws.amazon.com/cloudwatch/pricing/
[dynamodb streams]:
  https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html
[dynamodb point-in-time recovery]:
  https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/PointInTimeRecovery.html
[dynamodb pricing]: https://aws.amazon.com/dynamodb/pricing/on-demand/
[defining glue tables]:
  https://docs.aws.amazon.com/glue/latest/dg/tables-described.html
[s3 bucket policies]:
  https://docs.aws.amazon.com/AmazonS3/latest/dev/using-iam-policies.html
[using sse with cmks]:
  https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingKMSEncryption.html
[customer master keys]:
  https://docs.aws.amazon.com/kms/latest/developerguide/concepts.html#master_keys
[how to change a key policy]:
  https://docs.aws.amazon.com/kms/latest/developerguide/key-policy-modifying.html#key-policy-modifying-how-to
[cross account s3 access]:
  https://docs.aws.amazon.com/AmazonS3/latest/dev/example-walkthroughs-managing-access-example2.html
[cross account kms access]:
  https://docs.aws.amazon.com/kms/latest/developerguide/key-policy-modifying-external-accounts.html
[updating an ssm parameter]:
  https://docs.aws.amazon.com/systems-manager/latest/userguide/sysman-paramstore-cli.html
[deploy using the aws cli]:
  https://docs.aws.amazon.com/cli/latest/reference/cloudformation/deploy/index.html
[cloudformation console page]:
  https://console.aws.amazon.com/cloudformation/home
[changelog]: ../CHANGELOG.md
[on-demand backup and restore]:
  https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/BackupRestore.html
[aws data pipeline]: https://aws.amazon.com/datapipeline
[cognito advanced security]:
  https://docs.aws.amazon.com/cognito/latest/developerguide/cognito-user-pool-settings-advanced-security.html
[cloudfront access logging permissions]:
  https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/AccessLogs.html#AccessLogsBucketAndFileOwnership
[s3 access logging permissions]:
  https://docs.aws.amazon.com/AmazonS3/latest/dev/enable-logging-programming.html#grant-log-delivery-permissions-general
[limits]: LIMITS.md
[aws cloudformation stacksets]:
  https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/what-is-cfnstacksets.html
[cognito console]: https://console.aws.amazon.com/cognito
[managing users in user pools guide]:
  https://docs.aws.amazon.com/cognito/latest/developerguide/managing-users.html
[cognito rest api integration guide]:
  https://docs.aws.amazon.com/apigateway/latest/developerguide/apigateway-invoke-api-integrated-with-cognito-user-pool.html
[lake formation data permissions console]:
  https://docs.aws.amazon.com/lake-formation/latest/dg/granting-catalog-permissions.html
[exporting stack output values]:
  https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-stack-exports.html
[s3 lifecycle policies]:
  https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html
