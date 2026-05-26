# IBM Cloud

## Implementation

The `roachprod` implementation for IBM Cloud is based out of the IBM official [Golang SDK](https://github.com/IBM/platform-services-go-sdk).

### Account configuration

A few per-region / per-az resources need to be created to allow `roachprod`
to work properly:
- a single resource group for all roachprod resources
- one VPC per supported region
- one inbound firewall rules per VPC
- one subnets per AZ per VPC
- one public gateway per AZ per VPC for the instances internet access
- one single transit gateway with connections in each VPC

These resources are fetched/created on demand when the provider is used.
To speed up the provider's initialization on actions requiring only listing,
only the resource group will be fetched. During cluster creation, all resources
will be fetched and created.

This process requires the user setting up the account the first time to have
all required permissions to create the resources mentioned above.

### Floating IP addresses

Each floating IP address created is billed the monthly cost upfront.
To avoid being billed one IP address for each machine that we spin up, we simply
unbind the floating IP addresses when destroying the clusters and do not delete
the unbound  IP addresses.

This behavior allows us to have a pool of unbound IP addresses that we try and
reuse them during the subsequent clusters creations.

### Unsupported features

Unimplemented features supported on IBM Cloud:
- additional volumes management on a live cluster
- snapshots
- load balancer
- grow/shrink clusters

Unsupported features on IBM Cloud:
- local SSDs
- transient instances (a.k.a. `spot` instances)

## Interacting with IBM Cloud

While `roachprod` uses the Golang SDK, installing the CLI will be useful to
authenticate with IBM Cloud.

### Install the CLI and authenticate

Download the installer from the IBM Cloud CLI [Github repository](https://github.com/IBM-Cloud/ibm-cloud-cli-release/releases/).

Get a passcode from the IBM web console by hitting the profile icon in the upper right corner and choosing the `Log in to CLI and API` menu item.

Then, run the following `ibmcloud login` command displayed on screen.

```bash
ibmcloud login -a https://cloud.ibm.com -u passcode -p <redacted>
```

Generate an API key for `roachprod` use:
```bash
ibmcloud iam api-key-create roachprod
Creating API key roachprod under <redacted> as username@email.tld...
OK
API key roachprod was created

Please preserve the API key! It cannot be retrieved after it's created.

ID                   ApiKey-<redacted>
Name                 roachprod
Description          -
Locked               false
CRN                  <redacted>
Created At           2025-04-04T21:48+0000
Version              1-dcc0d478dab9a50b11e47cbb71d0bbc8
Disabled             false
Leaked               false
Action when leaked   disable
Session supported    false
API Key              <redacted>
```

Set the API Key as an environment variable: `IBM_APIKEY=<redacted>`.

N.B. that this is subject to change when SSO will be implemented.

### Extend the IBM Cloud functionalities

The base `IBM Cloud CLI` installation is close to an empty shell.
If you want to interact with IBM Cloud resources via the CLI, you will need to
extend its capabilities with plugins.

The following plugins will be required to manage infrastructure:
- vpc-infrastructure[infrastructure-service/is]

```bash
ibmcloud plugin install vpc-infrastructure
```

