# Roachprod Docker Image

This dockerfile will build roachprod from master and create an image
with the supporting CLI toolchains. The entrypoint for the image will
configure the CLI tools using files to be mounted into the `/secrets`
directory.

The easiest way to build this is to run the following from this directory.
```
gcloud builds submit -t gcr.io/cockroach-dev-inf/cockroachlabs/roachprod:master
```
