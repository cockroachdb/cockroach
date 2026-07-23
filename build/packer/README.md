# build/packer

This directory contains Packer templates that automate building VM images.
Each `.json` file is a packer template.

## To use

1. Find the build configuration you need in TeamCity under `Internal > Cockroach > Build > Infrastructure`
2. Trigger a run on the right branch

## To rotate GCP credentials
1. Authenticate using your `-a` account and generate a new key.
    ```
    gcloud iam service-accounts keys create new_packer_key.json \
        --iam-account=packer@crl-teamcity-agents.iam.gserviceaccount.com
    ```
2. Base64 encode the JSON content and copy it.
    ```
    cat new_packer_key.json | base64 | pbcopy
    ```
3. In TeamCity, update the value of `env.PACKER_SA_FILE` parameter under `GCP Service Account for Packer in crl-teamcity-agents` template
in `Internal > Cockroach > Build > Infrastructure` project.
4. Delete the key from your disk.
    ```
    rm new_packer_key.json
    ```
5. Remember to delete the old key from GCP to deactivate it.

## Template Basics

Each template specifies which cloud to build the image in, a base VM image
and one or more scripts that are run to configure the VM image.
