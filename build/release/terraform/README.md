# Release Automation Resources

This directory contains resource definitions used by release automation.

# Deployment

## Development deployments

Before you start hacking on your changes, make sure the dev environment is up-to-date:

```shell
  terraform init
  terraform workspace select dev
  terraform plan
  terraform apply
```

## Production deployments

Make sure you tested your changes in the dev environment before deploying.

```shell
  terraform workspace select prod
  terraform plan -var="project=dev-inf-prod" -var="bucket=release-automation-prod"
  terraform apply -var="project=dev-inf-prod" -var="bucket=release-automation-prod"
```

# Clients

Manually create a key for the `metadata-publisher@` account, and it can be used in TeamCity.

For the development environment:

```shell
gcloud --project dev-inf-devenv iam service-accounts keys create key-dev.json \
    --iam-account=metadata-publisher@dev-inf-devenv.iam.gserviceaccount.com
```

For the production environment:

```shell
gcloud --project dev-inf-prod iam service-accounts keys create key-prod.json \
    --iam-account=metadata-publisher@dev-inf-prod.iam.gserviceaccount.com
```
