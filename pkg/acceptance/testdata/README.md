# Acceptance Test Harnesses

The Dockerfile in this directory builds an image,
`us-east1-docker.pkg.dev/crl-ci-images/cockroach/acceptance`, in which we run
our language-specific acceptance tests. For each language we support, we
install the compiler or runtime, and typically the Postgres driver, into the
image. Where possible, we use packages provided by Debian; otherwise we invoke
the language's package manager while building the image.

In all cases, the language's package manager is removed or put into offline mode
before the image is fully baked to ensure that we don't accidentally introduce a
dependency on an external package repository into our CI pipeline. That means
that if you update a language's dependencies (e.g., `node/package.json`), you'll
need to update the image.

## Updating the `acceptance` image

- (One-time setup) Depending on how your Docker instance is configured, you may have to run `docker run --privileged --rm tonistiigi/binfmt --install all`. This will install `qemu` emulators on your system for platforms besides your native one.
- Ask someone from the Engineering Productivity team to build the image for
  both platforms and publish the cross-platform manifest by running
  `build-push-acceptance.sh`. Note that the non-native build for your image will
  be very slow since it will have to emulate.

No need to have your changes reviewed before you push an image, as we pin the
container version to use in `../util_docker.go`. Once your changes are ready for
review, update the pinned version and submit a PR.
