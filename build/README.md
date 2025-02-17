# Docker Deploy

Installing Docker is a prerequisite. The instructions differ depending on the
environment. Docker is comprised of two parts: the daemon server which runs on
Linux and accepts commands, and the client which is a Go program capable of
running on MacOS, all Unix variants and Windows.

## Docker Installation

Follow the [Docker install
instructions](https://docs.docker.com/engine/installation/).

## Available images

There are development and deploy images available.

### `bazel`/`bazelbuilder`

`us-east1-docker.pkg.dev/crl-ci-images/cockroach/bazel` is a Docker image containing basic requirements for
building the Cockroach binary using Bazel. It can be run using `./dev builder`.

Note that if you use this image, you should ensure that your Docker installation
grants 4GB or more of RAM to containers. On some systems, the default
configuration limits containers to 2GB memory usage and this can be insufficient
to build/link a CockroachDB executable.

There is a FIPS analogue to this image as well.

### Deployment

The deploy image is a downsized image containing a minimal environment for
running CockroachDB. It is based on RedHat's `ubi9/ubi-minimal` image and
contains only the main CockroachDB binary, libgeos libraries, and licenses. To
fetch this image, run `docker pull cockroachdb/cockroach` in the usual fashion.

To build the image yourself:

1. Fetch a cross-built version of `cockroach` from CI, or build one yourself
   using `./dev build cockroach geos --cross[=linuxarm]`.

1. Copy the necessary files into the `build/deploy` directory.

    ```sh
    cp ./artifacts/{cockroach,libgeos.so,libgeos_c.so} ./build/deploy
    cp ./LICENSE ./licenses/THIRD-PARTY-NOTICES.txt ./build/deploy
    ```

1. Build the CockroachDB Docker image.

    ```sh
    cd ./build/deploy
    docker build -t localhost/cockroach:latest .
    ```

# Updating toolchains

Cross toolchains may need to be rebuilt infrequently. Follow this process to
do so:

- Edit files in `build/toolchains/toolchainbuild` as desired.
- Run `build/toolchains/toolchainbuild/buildtoolchains.sh` to test --
  this will build the tarballs locally and place them in your
  `artifacts` directory.
- When you're happy with the result, commit your changes, submit a pull
  request, and have it reviewed.
- Ask someone with permissions to run the
  `Build and Publish Cross Toolchains` and
  `Build and Publish Darwin Toolchains` build configurations in TeamCity.
  These will publish the new toolchains to a new subdirectory in Google cloud
  storage, and the build log will additionally contain the sha256 of
  every tarball created.
- Update `build/toolchains/REPOSITORIES.bzl` with new sha256's for every
  toolchain, and `build/toolchains/crosstool-ng/toolchain.bzl` with new
  URL's.

# Updating the golang version

Please copy this checklist into the relevant commit message and perform these
steps:

* [ ] Adjust the Pebble tests to run in new version.
* [ ] Update [our `go` fork](https://github.com/cockroachdb/go) with a new branch containing our patches. Create a new branch `cockroach-go$GO_VERSION` and take note of the commit ID.
* [ ] Update `build/teamcity/internal/release/build-and-publish-patched-go/commit.txt` with the commit ID in the `go` fork.
* [ ] Update `build/teamcity/internal/release/build-and-publish-patched-go/impl.sh` with the new `GOVERS` and adjust SHA256 sums as necessary.
* [ ] Adjust `GO_FIPS_COMMIT` for the FIPS Go toolchain ([source](./teamcity/internal/release/build-and-publish-patched-go/impl-fips.sh)).
* [ ] Run the `Internal / Cockroach / Build / Toolchains / Publish Patched Go for Mac` build configuration in TeamCity with your latest version of the script above. Note the job depends on another job `Build and Publish Patched Go`. That job prints out the SHA256 of all tarballs, which you will need to copy-paste into `WORKSPACE` (see below). `Publish Patched Go for Mac` is an extra step that publishes the *signed* `go` binaries for macOS. That job also prints out the SHA256 of the Mac tarballs in particular.
* [ ] Adjust `--@io_bazel_rules_go//go/toolchain:sdk_version` in [.bazelrc](../.bazelrc).
* [ ] Bump the version in `WORKSPACE` under `go_download_sdk`. You may need to bump [rules_go](https://github.com/bazelbuild/rules_go/releases). Also edit the filenames listed in `sdks` and update all the hashes to match what you built in the step above.
* [ ] Bump the version in `WORKSPACE` under `go_download_sdk` for the FIPS version of Go (`go_sdk_fips`).
* [ ] Run `./dev generate bazel` to refresh `distdir_files.bzl`, then `bazel fetch @distdir//:archives` to ensure you've updated all hashes to the correct value.
* [ ] Bump the go version in `go.mod`.
* [ ] Bump the default installed version of Go in `bootstrap-debian.sh` ([source](./bootstrap/bootstrap-debian.sh)).
* [ ] Replace other mentions of the older version of go (grep for `golang:<old_version>` and `go<old_version>`).

# Updating the `bazelbuilder` image

The `bazelbuilder` image is used exclusively for performing builds using Bazel.
Only add dependencies to the image that are necessary for performing Bazel
builds. (Since the Bazel build downloads most dependencies as needed, updates to
the Bazel builder image should be very infrequent.) The `bazelbuilder` image is
published both for `amd64` and `arm64` platforms. To update the image, perform
the following steps:

- Edit `build/bazelbuilder/Dockerfile` as desired.
- Build the image by triggering the `Build and Push Bazel Builder Image` build in TeamCity. The generated image will be published to `us-east1-docker.pkg.dev/crl-ci-images/cockroach/bazel`.
- Update `build/.bazelbuilderversion` with the new tag and commit all your changes.
- Build the FIPS image by triggering the `Build and Push FIPS Bazel Builder Image` build in TeamCity. The generated image will be published to `us-east1-docker.pkg.dev/crl-ci-images/cockroach/bazel-fips`.
- Update `build/.bazelbuilderversion-fips` with the new tag and commit all your changes.
- Ensure the "Bazel CI" job passes on your PR before merging.

#  Dependencies

Dependencies are managed using `go mod`.

## Working Locally with Dependencies

### Installing a new Dependency

1. Familiarize yourself with [our wiki page on adding or updating
   dependencies](https://wiki.crdb.io/wiki/spaces/CRDB/pages/181862804/Adding%2C+Updating+and+Deleting+external+dependencies).
   In particular, you will need to go first through a non-technical step
   related to checking the license of the dependency. This is
   important and non-optional.
2. In `cockroachdb/cockroach`, switch to the local branch you plan to import the external package
   into
3. Run `go get <dependency>@latest`. To get a specific version, run `go get
   <dependency>@<version|branch|sha>`. You should see changes in `go.mod` when running `git diff`.

   Beware of not using `-u` (e.g. `go get -u`) as the `-u` flag also
   attempts to upgrade any indirect dependency from yours. We do not
   want (generally) to bundle indirect dep upgrades, which might
   include critical infrastructure packages like grpc, in the same PR
   as specific dep updates/installations. See the wiki page linked
   above for details.

   If the `go get` command still results in updates to important
   infrastructure dependencies besides the one you want to install,
   proceed as per the section "Upgrading important infrastructure
   dependencies" below.

4. Import the dependency to a go file in `cockroachdb/cockroach`. You may use an anonymous
   import, e.g. `import _ "golang.org/api/compute/v1"`, if you haven't written any code that
   references the dependency. This ensures `go mod tidy` will not delete your dependency.
   Note that IDEs may bicker that these import's paths don't exist. That's ok!
5. Run `go mod tidy` to ensure stale dependencies are removed.
6. Run `./dev generate bazel --mirror` to regenerate DEPS.bzl with the updated Go dependency information.
   Note that you need engineer permissions to mirror dependencies; if you want to get the Bazel build
   working locally without mirroring, `./dev generate bazel` will work, but you won't be able to check
   your changes in. (Assuming that you do have engineer permissions, you can run
   `gcloud auth application-default login` to authenticate if you get a credentials error.)
7. Run `./dev build short` to ensure your code compiles.

### Updating a Dependency

Follow the instructions for [Installing a Dependency](#installing-a-dependency).

Beware of not using `-u` (e.g. `go get -u`) as the `-u` flag also
attempts to upgrade any indirect dependency from yours. We do not
want (generally) to bundle indirect dep upgrades, which might
include critical infrastructure packages like grpc, in the same PR
as specific dep upgrades. See the wiki page linked above for details.

Note:

- You will still need to pay extra attention to the licensing step as
  described in [the wiki
  page](https://wiki.crdb.io/wiki/spaces/CRDB/pages/181862804/Adding%2C+Updating+and+Deleting+external+dependencies).
- If you're only importing a new package from an existing module in `go.mod`, you don't need to
  re-download the module, step 2 above.
- If you're only updating the package version, you probably don't need to update the import
  statements, step 3 above.
- If the `go get` command results in updates to important
  infrastructure dependencies besides the one you want to install,
  proceed as per "Upgrading important infrastructure dependencies"
  below.

### Updating important infrastructure dependencies

Sometimes a key, infrastructure-like dependency is upgraded as a
side-effect of (attempting to) installing or upgrading a more
lightweight dependency. See the [wiki
page](https://wiki.crdb.io/wiki/spaces/CRDB/pages/181862804/Adding+Updating+and+Deleting+external+dependencies#Reviewing-for-architectural-impact-of-key-dependencies)
for a list of what we consider key dependencies.

When this happens, we want to solicit more review scrutiny on this change.

To achieve this, proceed as follows:

1. If coming from the sections "Installing a new dep" or "updating a
   dep" above, check out a branch/commit prior to the original dep
   install/update you wanted to perform.
2. Perform the `go get ...@<version>` command only for those
   key dependencies you want/need to upgrade.
3. Proceed as per the rest of "Installing a new dependency" from step
   5 onwards.
4. Commit the result with a descriptive title, e.g. "deps: upgrade XXX
   to version YYY".
5. Push this change in a PR (also with a descriptive title), then
   solicit a review.

### Removing a dependency

When a dependency has been removed, run `go mod tidy` and `dev generate bazel`.
