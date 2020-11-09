# Production Readiness Guidelines

It is important to conduct your own testing prior to using this solution with
production data. The following guidelines provide steps you can follow to
mitigate against unexpected behaviours such as unwanted data loss, or unexpected
high spend that could arise by using the solution in an incompatible
configuration.

## 1. Review the Solution Limits

Consult the [Limits] guide to check your datasets are in a supported format and
your S3 bucket configuration is compatible with the solution requirements.

## 2. Learn about costs

Consult the [Cost Overview guide] to learn about the costs of running the
solution, and ways to set spend limits.

## 3. Deploy the solution in a test environment

We recommend first evaluating the solution by deploying it in an AWS account you
use for testing, with a sample of your dataset. After configuring the solution,
identify a set of queries to run against your dataset before and after [running
a Deletion Job].

> **Note:** You don't need to have a full copy of each dataset, but we recommend
> to have at least the same schema to make sure the test queries are as close to
> production as possible.

## 4. Run your test queries

These are examples of test queries:

- Count the total number of rows in a dataset (A)
- Count the number of rows that need to be deleted from the same dataset (B)
- Run a query to fetch one or more rows that won't be affected by deletion but
  contained in an object that will be rewritten because of other rows (C)

After running a deletion job:

- Repeat the first 2 queries to make sure the row count is correct:
  A<sub>1</sub>=A<sub>0</sub>-B<sub>0</sub> and B<sub>1</sub>=0
- Repeat the third query to ensure the rows have been re-written without
  affecting their schema (for instance, there is no unwanted type coercion
  against `date` or `number` types): C<sub>1</sub>=C<sub>0</sub>

If any error occurs or data doesn't match, review the [troubleshooting guide] or
check for [existing issues]. If you cannot find a resolution, feel free to [open
an issue].

## 4. Identify your own extra requirements

These guidelines are provided as suggested steps to identify your own acceptance
criteria, but they are not intended to be an exhaustive list. You should
consider testing any other factors that may apply to your workload before moving
to production. If you have any question please [open an issue]. We appreciate
your feedback.

## 5. Deploy the solution in production

For greater confidence, it could be a good idea to repeat the test queries in
production before/after a deletion job. If you would prefer some extra safety,
you can configure your data mappers to **not** delete the previous versions of
objects after write, so that if anything goes wrong you can manually recover
older versions of the objects; but remember to turn the setting back on after
you finish testing, and in case, perform a manual deletion of the previous
versions if so desired.

[cost overview guide]: COST_OVERVIEW.md
[existing issues]: https://github.com/awslabs/amazon-s3-find-and-forget/issues
[limits]: LIMITS.md
[open an issue]: https://github.com/awslabs/amazon-s3-find-and-forget/issues
[running a deletion job]: USER_GUIDE.md#running-a-deletion-job
[troubleshooting guide]: TROUBLESHOOTING.md
