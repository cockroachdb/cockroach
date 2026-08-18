# Roachprod GC container

This directory builds the roachprod image used by Cloud Run jobs. The image
contains roachprod plus the GCP and AWS CLIs that its providers still invoke.
Its entrypoint is the roachprod binary, so a Cloud Run job supplies arguments
beginning with `gc`.

The image does not log in to cloud providers or read credential files from a
`/secrets` volume:

- GCP uses the Cloud Run job's attached service account through Application
  Default Credentials and the metadata server.
- AWS uses `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and
  `AWS_REGION` (or `AWS_DEFAULT_REGION`).
- Azure uses `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, and
  `AZURE_TENANT_ID`; the Azure CLI is not installed.
- IBM Cloud continues to use the environment variables expected by the
  selected account configuration.
- Slack is optional. `--slack-token` defaults to `SLACK_TOKEN`.

Dead Man's Snitch reporting is intentionally outside this image. The scheduler
or workflow that starts the Cloud Run job should wait for its result and ping
the snitch only after a successful execution.

## Job arguments

A staging job that owns both its VMs and DNS zone can use:

```
gc
--clouds=gce
--gce-project=crl-e2e-infra-staging
--gce-infra-project=crl-e2e-infra-staging
```

The production job can select all providers:

```
gc
--clouds=gce,aws,azure,ibm
--gce-project=<all production GCE VM projects>
--gce-infra-project=crl-e2e-infra
--aws-account-ids=<account IDs>
--azure-subscription-names=<subscription names>
```

Every selected provider is required: the job exits unsuccessfully when one is
inactive or its inventory cannot be obtained. Jobs and ad-hoc runs that
intentionally target a subset must pass that subset explicitly through
`--clouds`.

DNS cleanup infers dangling records from the selected GCE inventory. Every GCE
project that creates records in the selected infra project's DNS zone must be
included in `--gce-project`; GC fails without deleting clusters or DNS records
when it cannot obtain a complete inventory.

## Publishing

Run `./push.sh` from this directory to build and publish an image. The script
submits the current clean checkout to Cloud Build and only publishes the image;
deployment is owned by the infrastructure repository. Refusing dirty checkouts
keeps the image's commit-SHA tag traceable to its source.

Cloud Run jobs should use an immutable digest or commit-SHA tag supplied by
Terraform. Do not use `:latest`: Cloud Run resolves tags when a job is updated,
not before each scheduled execution.
