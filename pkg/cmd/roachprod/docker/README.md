# Roachprod Docker Image

This dockerfile will build roachprod and create an image with the supporting
CLI toolchains. The entrypoint for the image will configure the CLI tools using
files to be mounted into the `/secrets` directory.

In order to update the kubernetes cron job, run `./push` in the current
directory.
