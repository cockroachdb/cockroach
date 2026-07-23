# `roachprod` AWS Configuration

## Overview

EC2 on AWS requires a bunch of per-region and per-AZ resources in order to
operate properly. In particular, we need to set up an AMI and security group
per region and a subnet per AZ. We also need to set up peering between each
pair of available AZs.

This set of resources is provided to `roachprod` via  a configuration struct
serialized as JSON (see `config.go`). A bespoke config can be provided to
`roachprod` by passing a file path to `--aws-config`. The default config is
compiled into the `roachprod` binary using `go-bindata` (see the go:generate
comments in `config.go`).

In order to manage and create these resources, we use terraform. However, even
that is sort of painful given we need to construct all the pairings there too.
A small utility, `terraformgen`, generates `terraform/main.tf` based on its
hard-coded listing of regions and other AWS account properties. The configuration
of per-region and peering setups live in `terraform`.

## Regeneration

Regenerating the resources and producing the `config.json` file requires
running terraform. Unfortunately, terraform changed its type system between
versions 0.11 and 0.12. The `tf` files used by this tool are in the `0.11`
format. In order to regenerate, you need to use `0.11`.

In order to sync the resources with AWS, run `terraform` from the `terraform`
directory.

```bash
terraform init
```

```bash
terraform apply
```

```bash
terraform output --json > ../config.json
```

