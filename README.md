Amazon S3 Find and Forget
=========================

> Warning: This project is currently in beta and should not be used for
> production purposes.

Amazon S3 Find and Forget is a solution to the need to selectively erase
records from data lakes stored on Amazon Simple Storage Service (Amazon S3).
This solution can assist data lake operators to fulfill their obligations to
handle erasure requests such as those that can be made under the European
General Data Protection Regulation (GDPR).

The Amazon S3 Find and Forget solution can be used with Parquet-format data
stored in Amazon S3 buckets. Your data lake is connected to the solution via an
AWS Glue table and specifying which columns in the table contain user
identifiers.

Once configured, you can queue record identifiers that you want the
corresponding data erased for. You can then run a deletion job to remove the
data corresponding to the records specified from the objects in the data lake.
A report log is provided of all the S3 objects modified.

The solution provides a web user interface, and a REST API to allow you to
integrate it in your own applications.

## Documentation
- [User Guide](docs/USER_GUIDE.md)
- [Deployment](docs/USER_GUIDE.md#deploying-the-solution)
- [Monitoring the Solution](docs/MONITORING.md)
- [Local Development](docs/LOCAL_DEVELOPMENT.md)
- [Limits](docs/LIMITS.md)
