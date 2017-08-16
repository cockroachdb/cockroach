# build/packer

This directory contains [Packer] templates that automate building VM images.
To use, install Packer, then run:

```
$ packer build VM-TEMPLATE.json
```

The location of the created VM image will be printed when the build completes.

At present, the only VM template available builds TeamCity agents. You'll need
`DIGITALOCEAN_API_TOKEN` set in your environment. Employees can find the token
in customenv.mk. You'll also need to use [Nikhil's fork of
Packer][benesch-packer] until [packer#5527] lands.

[Packer]: https://www.packer.io
[benesch-packer]: https://github.com/benesch/packer/tree/digitalocean
[packer#5527]: https://github.com/hashicorp/packer/pull/5527
