# Acceptance Test Harnesses

The Dockerfile in this directory builds an image, `cockroachdb/acceptance`, in
which we run our language-specific acceptance tests. For each language we
support, we install the compiler or runtime, and typically the Postgres driver,
into the image. Where possible, we use packages provided by Debian; otherwise we
invoke the language's package manager while building the image.

In all cases, the language's package manager is removed or put into offline mode
before the image is fully baked to ensure that we don't accidentally introduce a
dependency on an external package repository into our CI pipeline. That means
that if you update a language's dependencies (e.g., `node/package.json`), you'll
need to update the image.

To build a new acceptance image:

```bash
$ docker build -t cockroachdb/acceptance .
```

To push the just-built acceptance image to the registry:

```bash
$ version=$(date +%Y%m%d-%H%M%S)
$ docker tag cockroachdb/acceptance cockroachdb/acceptance:$version
$ docker push cockroachdb/acceptance:$version
```

No need to have your changes reviewed before you push an image, as we pin the
container version to use in `../util_docker.go`. Once your changes are ready for
review, update the pinned version and submit a PR.
