Amazon S3 Find and Forget
=========================

> Warning: This project is currently being developed and the code shouldn't be used in production.

Amazon S3 Find and Forget is a solution for helping customers to find and delete user data in Amazon S3.
It is designed to help customers to fulfil their GDPR’s Right To Be Forgotten obligations when operating in a large Data Lake in a performant, reliable, secure and cost-effective way.

- [Deployment](#deploy)
- [Monitoring the Solution](docs/MONITORING.md)
- [Automated Tests](docs/TESTING.md)

## Getting Started

### Deploy

Pre-requirements:
* AWS CLI
* Python 3.7.5 and pip
* virtualenv

1. Install all the dependencies

```bash
make setup
```

2. Deploy

```bash
make deploy
```

### Local Development

#### Deletion Task
1. Build the image locally
```bash
docker build -f backend/ecs_tasks/delete_files/Dockerfile -t s3f2 .
```

2. Run the container using Make
```bash
make run-local-container
```
If you wish to override the default profile being used:
```bash
make run-local-container AWS_PROFILE=my-profile
```