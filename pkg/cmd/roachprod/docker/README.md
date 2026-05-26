# Roachprod Docker Image

This dockerfile will build roachprod and create an image with the supporting
CLI toolchains. The entrypoint for the image will configure the CLI tools using
files to be mounted into the `/secrets` directory.

In order to update the kubernetes cron job, run `./push.sh` in the current
directory. Make sure that `kubectl` is available before running `./push.sh`.

In order to test the docker build process, you can run the same script with
`OWNER` and/or `REPO` set: `OWNER=rail ./push.sh`. Note, that the built image
won't replace the existing Kubernetes image.
