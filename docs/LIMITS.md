# Limits

This section describes current limitations of the Amazon S3 Find and Forget
solution. We are actively working on adding additional features and supporting
more data formats. For feature requests, please open an issue on
our [Issue Tracker].

#### Supported Data Formats

The following data formats are supported:

| Data Format | Compression |
| --- | --- |
| Apache Parquet | SNAPPY (default for Apache Parquet) |

#### Supported Query Providers

The following data catalog provider and query executor combinations are
supported:

| Catalog Provider | Query Executor |
| --- | --- |
| AWS Glue | Amazon Athena |

#### Concurrency Limits

| Catalog Provider | Query Executor |
| --- | --- |
| Max Concurrent Jobs | 1 |
| Max Athena Concurrency | See account service quota |
| Max Fargate Concurrency | See account service quota |

#### Other Limitations

- Only buckets with versioning set to **Enabled** are supported
- Decompressed individual object size must be less than the Fargate task memory
limit (`DeletionTaskMemory`) specified when launching the stack
- The bucket targeted by a data mapper must be in the same region as the
Amazon S3 Find and Forget deployment
- S3 Objects encrypted with SSE-C are not supported
- If the bucket targeted by a data mapper belongs to an account other than
the account that the Amazon S3 Find and Forget Solution is deployed in,
a CMK must be used for encryption
- After a deletion occurs, the S3 object owner will always be the account that
the Amazon S3 Find and Forget solution is deployed in. The previous owner will
also be granted `FULL_ACCESS`

#### Service Quotas

If you wish to increase the number of concurrent queries that can be by
Athena and therefore speed up the Find phase, you will need to request a
Service Quota increase for Athena. For more, information consult the
[Athena Service Quotas] page. Similarly, to increase the number of concurrent
Fargate tasks and therefore speed up the Forget phase, consult the
[Fargate Service Quotas] page. When configuring the solution, you should not
set an `AthenaConcurrencyLimit` or `DeletionTasksMaxNumber` greater than the
respective Service Quote for your account.

Amazon S3 Find and Forget is also bound by any other service quotas which apply
to the underlying AWS services that it leverages. For more information,
consult the AWS docs for [Service Quotas] and the relevant Service Quota page
for the service in question:

- [SQS Service Quotas]
- [Step Functions Service Quotas]
- [DynamoDB Service Quotas]

[Issue Tracker]: https://github.com/awslabs/amazon-s3-find-and-forget/issues
[Service Quotas]: https://docs.aws.amazon.com/general/latest/gr/aws_service_limits.html
[Service Quotas]: https://docs.aws.amazon.com/general/latest/gr/aws_service_limits.html
[Athena Service Quotas]: https://docs.aws.amazon.com/athena/latest/ug/service-limits.html
[Fargate Service Quotas]: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/service-quotas.html
[Step Functions Service Quotas]: https://docs.aws.amazon.com/step-functions/latest/dg/limits.html
[SQS Service Quotas]: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-quotas.html
[DynamoDB Service Quotas]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html
