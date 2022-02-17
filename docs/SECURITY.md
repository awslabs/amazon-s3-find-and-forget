# Security

When you build systems on AWS infrastructure, security responsibilities are
shared between you and AWS. This shared model can reduce your operational burden
as AWS operates, manages, and controls the components from the host operating
system and virtualization layer down to the physical security of the facilities
in which the services operate. For more information about security on AWS, visit
the [AWS Security Center].

## IAM Roles

AWS Identity and Access Management (IAM) roles enable customers to assign
granular access policies and permissions to services and users on AWS. This
solution creates several IAM roles, including roles that grant the solution’s
AWS Lambda functions access to the other AWS services used in this solution.

## Amazon Cognito

Amazon Cognito is used for managing access to the web user interface and the
API. For more information, consult [Accessing the application].

## Amazon CloudFront

This solution deploys a static website hosted in an Amazon S3 bucket. To help
reduce latency and improve security, this solution includes an Amazon CloudFront
distribution with an origin access identity, which is a special CloudFront user
that helps restrict access to the solution’s website bucket contents. For more
information, see [Restricting Access to Amazon S3 Content by Using an Origin
Access Identity].

If you wish to increase the security of the web user interface or the API, we
recommend considering integrating [AWS WAF], which gives you control over how
traffic reaches your applications by enabling you to create security rules such
as filtering out specific traffic patterns you define.

[aws security center]: https://aws.amazon.com/security
[aws waf]: https://aws.amazon.com/waf
[accessing the application]: USER_GUIDE.md#accessing-the-application
[restricting access to amazon s3 content by using an origin access identity]: https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/private-content-restricting-access-to-s3.html
