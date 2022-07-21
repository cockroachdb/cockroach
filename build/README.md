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
running CockroachDB. It is based on RedHat's `ubi8/ubi-minimal` image and
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
go/src/github.com/cockroachdb/cockroach $ cp -r licenses build/deploy/
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
* [ ] Adjust version in Docker image ([source](./builder/Dockerfile)).
* [ ] Adjust version in the TeamCity agent image ([setup script](./packer/teamcity-agent.sh))
* [ ] Rebuild and push the Docker image (following [Basic Process](#basic-process))
* [ ] Update `build/teamcity/internal/release/build-and-publish-patched-go/impl.sh` with the new version and adjust SHA256 sums as necessary.
* [ ] Run the `Internal / Release / Build and Publish Patched Go` build configuration in TeamCity with your latest version of the script above. This will print out the new URL's and SHA256 sums for the patched Go that you built above.
* [ ] Bump the version in `WORKSPACE` under `go_download_sdk`. You may need to bump [rules_go](https://github.com/bazelbuild/rules_go/releases). Also edit the filenames listed in `sdks` and update all the hashes to match what you built in the step above.
* [ ] Run `./dev generate bazel` to refresh `distdir_files.bzl`, then `bazel fetch @distdir//:archives` to ensure you've updated all hashes to the correct value.
* [ ] Bump the version in `builder.sh` accordingly ([source](./builder.sh#L6)).
* [ ] Bump the version in `go-version-check.sh` ([source](./go-version-check.sh)), unless bumping to a new patch release.
* [ ] Bump the go version in `go.mod`. You may also need to rerun `make vendor_rebuild` if vendoring has changed.
* [ ] Bump the default installed version of Go in `bootstrap-debian.sh` ([source](./bootstrap/bootstrap-debian.sh)).
* [ ] Replace other mentions of the older version of go (grep for `golang:<old_version>` and `go<old_version>`).
* [ ] Update the `builder.dockerImage` parameter in the TeamCity [`Cockroach`](https://teamcity.cockroachdb.com/admin/editProject.html?projectId=Cockroach&tab=projectParams) and [`Internal`](https://teamcity.cockroachdb.com/admin/editProject.html?projectId=Internal&tab=projectParams) projects.
* [ ] Ask the Developer Infrastructure team to deploy new TeamCity agent images according to [packer/README.md](./packer/README.md)

You can test the new builder image in TeamCity by using the custom parameters
UI (the "..." icon next to the "Run" button) to verify the image before
committing the change.

## Updating the nodejs version

Please follow the instructions above on updating the golang version, omitting the go-version-check.sh step.

## Updating the `bazelbuilder` image

The `bazelbuilder` image is used exclusively for performing builds using Bazel. Only add dependencies to the image that are necessary for performing Bazel builds. (Since the Bazel build downloads most dependencies as needed, updates to the Bazel builder image should be very infrequent.) The `bazelbuilder` image is published both for `amd64` and `arm64` platforms. You can go through the process of publishing a new Bazel build

- (One-time setup) Depending on how your Docker instance is configured, you may have to run `docker run --privileged --rm tonistiigi/binfmt --install all`. This will install `qemu` emulators on your system for platforms besides your native one.
- Edit `build/bazelbuilder/Dockerfile` as desired.
- Build the image for both platforms and publish the cross-platform manifest. Note that the non-native build for your image will be very slow since it will have to emulate.
```
    TAG=$(date +%Y%m%d-%H%M%S)
    docker build --platform linux/amd64 -t cockroachdb/bazel:amd64-$TAG build/bazelbuilder
    docker push cockroachdb/bazel:amd64-$TAG
    docker build --platform linux/arm64 -t cockroachdb/bazel:arm64-$TAG build/bazelbuilder
    docker push cockroachdb/bazel:arm64-$TAG
    docker manifest create cockroachdb/bazel:$TAG --amend cockroachdb/bazel:amd64-$TAG --amend cockroachdb/bazel:arm64-$TAG
    docker manifest push cockroachdb/bazel:$TAG
```
- Then, update `build/teamcity-bazel-support.sh` with the new tag and commit all your changes.
- Ensure the "Bazel CI" job passes on your PR before merging.

#  Dependencies

Dependencies are managed using `go mod`. We use `go mod vendor` so that we can import and use
non-Go files (e.g. protobuf files) using the [modvendor](https://github.com/goware/modvendor)
script. Adding or updating a dependecy is a two step process: 1) import the dependency in a go
file on your local branch, 2) push a commit containing this import to the `vendored` git submodule.

## Working Locally with Dependencies

### Installing a Dependency
1. In `cockroachdb/cockroach`, switch to the local branch you plan to import the external package
   into
2. Run `go get -u <dependency>`. To get a specific version, run `go get -u
   <dependency>@<version|branch|sha>`. You should see changes in `go.mod` when running `git diff`.
3. Import the dependency to a go file in `cockorachdb/cockroach`. You may use an anonymous
   import, e.g. `import _ "golang.org/api/compute/v1"`, if you haven't written any code that
   references the dependency. This ensures cockroach's make file will properly add the package(s) to the vendor directory. Note that IDEs may bicker that
   these import's paths don't exist. That's ok!
4. Run `go mod tidy` to ensure stale dependencies are removed.
5. Run `make vendor_rebuild` to add the package to the vendor directory. Note this command will only
   add packages you have imported in the codebase (and any of the package's dependencies), so you
   may want to add import statements for each package you plan to use (i.e. repeat step 3 a couple times).
6. Run `cd vendor && git diff && cd ..`  to ensure the vendor directory contains the package(s)
   you imported
7. Run `make buildshort` to ensure your code compiles.
8. Run `./dev generate bazel --mirror` to regenerate DEPS.bzl with the updated Go dependency information.
   Note that you need engineer permissions to mirror dependencies; if you want to get the Bazel build
   working locally without mirroring, `./dev generate bazel` will work, but you won't be able to check
   your changes in. (Assuming that you do have engineer permissions, you can run
   `gcloud auth application-default login` to authenticate if you get a credentials error.)
9. Follow instructions for [pushing the dependency to the `vendored` submodule](#pushing-the-dependency-to-the-vendored-submodule)

### Updating a Dependency
Follow the instructions for [Installing a Dependency](#installing-a-dependency). Note:
- If you're only importing a new package from an existing module in `go.mod`, you don't need to
   re-download the module, step 2 above.
- If you're only updating the package version, you probably don't need to update the import
   statements, step 3 above.

When [pushing the dependency to the `vendored` submodule](#pushing-the-dependency-to-the-vendored-submodule), you may either checkout a new branch, or create a new commit in the original branch you used to publicize the vendor
  dependency.

### Removing a dependency
When a dependency has been removed, run `go mod tidy` and then `make vendor_rebuild`.
Then follow the [Pushing the Dependency to the `vendored` submodule](#pushing-the-dependency-to-the-vendored-submodule) steps.

## Working With Submodules
To keep the bloat of all the changes in all our dependencies out of our main
repository, we embed `vendor` as a git submodule, storing its content and
history in [`vendored`](https://github.com/cockroachdb/vendored) instead.

This split across two repositories however means that changes involving
changed dependencies require a two step process. After altering dependencies and making related code
changes, follow the steps below.

### Pushing the Dependency to the `vendored` submodule
- Notice that `git status` in `cockroachdb/cockroach` checkout will report that the
`vendor` submodule has `modified/untracked content`.

- `cd` into `vendor`, and ...
    + Checkout a **new** named branch
    + Run `git add .`
    + Commit all changes, with a nice short message. There's no explicit policy related to commit
      messages in the vendored submodule.

- At this point the `git status` in your `cockroachdb/cockroach` checkout will report `new commits`
for `vendor` instead of `modified content`.
- Back in your `cockroachdb/cockroach` branch, commit your code changes and the new `vendor`
  submodule ref.

- Before the `cockroachdb/cockroach` commit can be submitted in a pull request, the submodule commit
  it references must be available on `github.com/cockroachdb/vendored`. So, when you're ready to
  publicize your vendor changes, push the `vendored` commit to remote:

  + Organization members can push their named branches there directly, via:
    + `git push [remote vendor name, probably 'origin'] [your vendor branch] `
  + Non-members should fork the `vendored` repo and submit a pull request to
`cockroachdb/vendored`, and need wait for it to merge before they will be able
to use it in a `cockroachdb/cockroach` PR.

### `master` Branch Pointer in Vendored Repo

Since the `cockroachdb/cockroach` submodule references individual commit
hashes in `vendored`, there is little significance to the `master` branch in
`vendored` -- as outlined above, new commits are always authored with the
previously referenced commit as their parent, regardless of what `master`
happens to be.

It is critical that any ref in vendored that is referenced from `cockroachdb/cockroach` remain
available in vendored in perpetuity. One way to ensure this is to leave the vendored branch that
you pushed your changes to in place.

If you would like to delete your feature branch in the vendored repository, you must first ensure
that another branch in vendored contains the commit referenced by `cockroachdb/cockroach`. You can
update the master branch in vendored to point at the git SHA currently referenced in
`cockroachdb/cockroach`.

### Conflicting Submodule Changes

If you pull/rebase from `cockroach/cockroachdb` and encounter a conflict in the vendor directory,
it is often easiest to take the master branch's vendored directory and then recreate your vendor
changes on top of it. For example:

1. Remove your local changes to `vendored` by resetting your local
   vendor directory to the commit currently used by `origin/master` on
   `cockroachdb/cockroach`.
      + Get reference: `git ls-tree origin/master vendor | awk '{print $3}'`
      + Reset to it: `cd vendor && git reset --hard REF`
2. In `cockroach/cockroachdb`, amend the commit that contained the dirty vendor pointer.
3. Try pulling/rebasing again, and if that works, rebuild your local vendor repo with
`go mod tidy` and `make vendor_rebuild`
4. Push the clean vendor changes to the remote vendor submodule, following the [Pushing the Dependency to the `vendored` submodule](#pushing-the-dependency-to-the-vendored-submodule)

Note: you may also observe conflicts in `go.mod` and `go.sum`. Resolve the conflict like
any vanilla conflict on `cockroach/cockroachdb`, preferring master's
version. Then, `make vendor_rebuild` to re-add your local changes to `go.
mod` and `go.sum`.
### Recovering from a broken vendor directory

If you happen to run into a broken `vendor` directory which is irrecoverable,
you can run the following commands which will restore the directory in
working order:

```
rm -rf vendor
git checkout HEAD vendor # you can replace HEAD with any branch/sha
git submodule update --init --recursive
```

### Repository Name

We only want the vendor directory used by builds when it is explicitly checked
out *and managed* as a submodule at `./vendor`.

If a go build fails to find a dependency in `./vendor`, it will continue
searching anything named "vendor" in parent directories. Thus the vendor
repository is _not_ named "vendor", to minimize the risk of it ending up
somewhere in `GOPATH` with the name `vendor` (e.g. if it is manually cloned),
where it could end up being unintentionally used by builds and causing
confusion.
