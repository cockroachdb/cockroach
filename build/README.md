# Docker Deploy

Installing docker is a prerequisite. The instructions differ depending on the
environment. Docker is comprised of two parts: the daemon server which runs on
Linux and accepts commands, and the client which is a Go program capable of
running on MacOS, all Unix variants and Windows.

## Docker Installation

Follow the [Docker install
instructions](https://docs.docker.com/engine/installation/).

## Available images

There are development and deploy images available.

### Development

The development image is a bulky image containing a complete build toolchain.
It is well suited to hacking around and running the tests (including the
acceptance tests). To fetch this image, run `./builder.sh pull`. The image can
be run conveniently via `./builder.sh`.

Note that if you use the builder image, you should ensure that your
Docker installation grants 4GB or more of RAM to containers. On some
systems, the default configuration limits containers to 2GB memory
usage and this can be insufficient to build/link a CockroachDB
executable.

### Deployment

The deploy image is a downsized image containing a minimal environment for
running CockroachDB. It is based on RedHat's `ubi9/ubi-minimal` image and
contains only the main CockroachDB binary, libgeos libraries, and licenses. To
fetch this image, run `docker pull cockroachdb/cockroach` in the usual fashion.

To build the image yourself, use the Dockerfile in the `deploy` directory after
building a release version of the binary with the development image described in
the previous section. The CockroachDB binary will be built inside of that
development container, then placed into the minimal deployment container. The
resulting image `cockroachdb/cockroach` can be run via `docker run` in the
usual fashion. To be more specific, the steps to do this are:

```
go/src/github.com/cockroachdb/cockroach $ ./build/builder.sh mkrelease linux-gnu
go/src/github.com/cockroachdb/cockroach $ cp ./cockroach-linux-2.6.32-gnu-amd64 build/deploy/cockroach
go/src/github.com/cockroachdb/cockroach $ cp ./lib.docker_amd64/libgeos_c.so ./lib.docker_amd64/libgeos.so build/deploy/
go/src/github.com/cockroachdb/cockroach $ cp ./LICENSE ./licenses/THIRD-PARTY-NOTICES.txt build/deploy/
go/src/github.com/cockroachdb/cockroach $ cd build/deploy && docker build -t cockroachdb/cockroach .
```

The list of valid/recognized targets is available in the script
`build/build/mkrelease.sh`, for example `amd64-linux-gnu` and
`amd64-darwin`. Note that this script supports experimental targets
which may or may not work (and are not officially supported).

# Upgrading / extending the Docker image

## Toolchains

The `cockroachdb/builder` image has a number of cross-compilers
installed for various targets. We build those cross-compilers in a
separate step prior to the actual image build, and pull in tarballs
to install them during the image build. This saves time and allows us to
use the same toolchains for the Bazel build.

Toolchains may need to be rebuilt infrequently. Follow this process to
do so (if you don't need to update the toolchains, proceed to "basic
process" below):

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
- Update the URL's in `build/builder/Dockerfile` and their sha256's
  accordingly. Then proceed to follow the "Basic process" steps below.

## Basic Process

NOTE: This describes how to update the old-fashioned `make`-based builder
image. This is no longer maintained. Wherever it's used, you should stop
using it.

- Edit `build/builder/Dockerfile` as desired.
- Run `build/builder.sh init` to test -- this will build the image locally.
  The result of `init` is a docker image version which you can subsequently
  stick into the `version` variable inside the `builder.sh` script for
  testing locally.
- When you're happy with the result, commit your changes, submit a pull request,
  and have it reviewed.
- Ask someone with permissions to run the `Build and Push new Builder Image`
  build configuration in TeamCity. This will build and push the new Docker image
  to [DockerHub](https://hub.docker.com/repository/docker/cockroachdb/builder).
- Copy the tag of the new image (which will look like `YYYYMMDD-NNNNNN`) into
  `build/builder.sh`, re-commit your changes, and update the pull request. You
  can now merge the pull request once you've got a sign-off.
- Finally, use the tag of the new image to update the `builder.dockerImage`
  configuration parameter in TeamCity under the [`Cockroach`](https://teamcity.cockroachdb.com/admin/editProject.html?projectId=Cockroach&tab=projectParams) and [`Internal`](https://teamcity.cockroachdb.com/admin/editProject.html?projectId=Internal&tab=projectParams) projects.

## Updating the golang version

Please copy this checklist (based on [Basic Process](#basic-process)) into the relevant commit message, with a link
back to this document and perform these steps:

* [ ] Adjust the Pebble tests to run in new version.
* [ ] Adjust version in the TeamCity agent image ([setup script](./packer/teamcity-agent.sh))
* [ ] Update `build/teamcity/internal/release/build-and-publish-patched-go/impl.sh` with the new version and adjust SHA256 sums as necessary.
* [ ] Adjust `GO_VERSION` and `GO_FIPS_COMMIT` for the FIPS Go toolchain ([source](./teamcity/internal/release/build-and-publish-patched-go/impl-fips.sh)).
* [ ] Run the `Internal / Cockroach / Build / Toolchains / Publish Patched Go for Mac` build configuration in TeamCity with your latest version of the script above. Note the job depends on another job `Build and Publish Patched Go`. That job prints out the SHA256 of all tarballs, which you will need to copy-paste into `WORKSPACE` (see below). `Publish Patched Go for Mac` is an extra step that publishes the *signed* `go` binaries for macOS. That job also prints out the SHA256 of the Mac tarballs in particular.
* [ ] Adjust `--@io_bazel_rules_go//go/toolchain:sdk_version` in [.bazelrc](../.bazelrc).
* [ ] Bump the version in `WORKSPACE` under `go_download_sdk`. You may need to bump [rules_go](https://github.com/bazelbuild/rules_go/releases). Also edit the filenames listed in `sdks` and update all the hashes to match what you built in the step above.
* [ ] Bump the version in `WORKSPACE` under `go_download_sdk` for the FIPS version of Go (`go_sdk_fips`).
* [ ] Run `./dev generate bazel` to refresh `distdir_files.bzl`, then `bazel fetch @distdir//:archives` to ensure you've updated all hashes to the correct value.
* [ ] Bump the go version in `go.mod`.
* [ ] Bump the default installed version of Go in `bootstrap-debian.sh` ([source](./bootstrap/bootstrap-debian.sh)).
* [ ] Replace other mentions of the older version of go (grep for `golang:<old_version>` and `go<old_version>`).
* [ ] Ask the Developer Infrastructure team to deploy new TeamCity agent images according to [packer/README.md](./packer/README.md)

You can test the new builder image in TeamCity by using the custom parameters
UI (the "..." icon next to the "Run" button) to verify the image before
committing the change.

## Updating the nodejs version

Please follow the instructions above on updating the golang version, omitting the go-version-check.sh step.

## Updating the `bazelbuilder` image

The `bazelbuilder` image is used exclusively for performing builds using Bazel. Only add dependencies to the image that are necessary for performing Bazel builds. (Since the Bazel build downloads most dependencies as needed, updates to the Bazel builder image should be very infrequent.) The `bazelbuilder` image is published both for `amd64` and `arm64` platforms. You can go through the process of publishing a new Bazel build

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
