# build/packer

This directory contains [Packer] templates that automate building VM images.
To use, install Packer, then run:

```
$ packer build VM-TEMPLATE.json
```

The location of the created VM image will be printed when the build completes.

At present, the only VM template available builds TeamCity agent images for
Google Compute Engine. You'll need to be either authenticated with the `gcloud`
tool or provide your Google Cloud JSON credentials in a [known location][gauth].


[Packer]: https://www.packer.io
[gauth]: https://www.packer.io/docs/builders/googlecompute.html#running-without-a-compute-engine-service-account
