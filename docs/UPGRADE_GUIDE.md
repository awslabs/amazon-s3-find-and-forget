# Upgrade Guide

## Migrating from <=v0.8 to 0.9

The default behaviour of the solution has been changed in v0.9 to deploy and use
the VPC that has been purpose-built for the solution as a nested stack.

If you have deployed the standalone VPC stack provided in previous versions, you
should should set `DeployVpc` to **true** when upgrading to v0.9 and input the
same values for the `FlowLogsGroup` and `FlowLogsRoleArn`. parameters that were
used when deploying the standalone VPC stack. After the deployment of v0.9 is
complete, you may delete the old VPC stack.

If you wish to continue using an existing VPC, you must set `DeployVpc` to
**false** when upgrading to v0.9.
