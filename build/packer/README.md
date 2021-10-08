# build/packer

This directory contains [Packer] templates that automate building VM images.
Each `.json` file is a packer template.

## To use

1. Install Packer:
    ```bash
    brew install packer
    ```
2. Configure `gcloud` with your personal [User Application Default Credentials][gauth].
3. Run:
   ```bash
   packer build <VM_TEMPLATE>.json
   ```

The location of the created VM image will be printed when the build completes.

## Template Basics

Each template specifies which cloud to build the image in, a base VM image
and one or more scripts that are run to configure the VM image.

## Available VMs

At present, there is only VM template available and it builds TeamCity agent
images for Google Compute Engine. You'll need to be either authenticated with
the `gcloud` tool or provide your Google Cloud JSON credentials in a
[known location][gauth].


[Packer]: https://www.packer.io
[gauth]: https://www.packer.io/docs/builders/googlecompute#running-locally-on-your-workstation
