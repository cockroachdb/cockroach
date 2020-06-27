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
running CockroachDB. It is based on Debian Jessie and contains only the main
CockroachDB binary. To fetch this image, run `docker pull
cockroachdb/cockroach` in the usual fashion.

To build the image yourself, use the Dockerfile in the `deploy` directory after
building a release version of the binary with the development image described in
the previous section. The CockroachDB binary will be built inside of that
development container, then placed into the minimal deployment container. The
resulting image `cockroachdb/cockroach` can be run via `docker run` in the
usual fashion. To be more specific, the steps to do this are:

```
go/src/github.com/cockroachdb/cockroach $ ./build/builder.sh mkrelease linux-gnu
go/src/github.com/cockroachdb/cockroach $ cp ./cockroach-linux-2.6.32-gnu-amd64 build/deploy/cockroach
go/src/github.com/cockroachdb/cockroach $ cd build/deploy && docker build -t cockroachdb/cockroach .
```

The list of valid/recognized targets is available in the script
`build/build/mkrelease.sh`, for example `amd64-linux-gnu` and
`amd64-darwin`. Note that this script supports experimental targets
which may or may not work (and are not officially supported).

# Upgrading / extending the Docker image

## Basic Process

- Edit `build/builder/Dockerfile` as desired
- Run `build/builder.sh init` to test -- this will build the image locally. Beware this can take a lot of time. The result of `init` is a docker image version which you can subsequently stick into the `version` variable inside the `builder.sh` script for testing locally.
- Once you are happy with the result, run `build/builder.sh push` which pushes your image towards Docker hub, so that it becomes available to others. The result is again a version number, which you then *must* copy back into `builder.sh`. Then commit the change to both Dockerfile and `builder.sh` and submit a PR.
- Finally, use this version number to update the `builder.dockerImage` configuration parameter in TeamCity under the [`Cockroach`](https://teamcity.cockroachdb.com/admin/editProject.html?projectId=Cockroach&tab=projectParams) and [`Internal`](https://teamcity.cockroachdb.com/admin/editProject.html?projectId=Internal&tab=projectParams) projects.

## Updating the golang version

Please copy this checklist (based on [Basic Process](#basic-process)) into the relevant commit message, with a link
back to this document and perform these steps:

* [ ] Adjust the Pebble tests to run in 1.14.
* [ ] Adjust version in Docker image ([source](./builder/Dockerfile#L199-L200)).
* [ ] Rebuild and push the Docker image (following [Basic Process](#basic-process))
* [ ] Bump the version in `builder.sh` accordingly ([source](./builder.sh#L6)).
* [ ] Bump the version in `go-version-check.sh` ([source](./go-version-check.sh)), unless bumping to a new patch release.
* [ ] Bump the go version in `go.mod`. You may also need to rerun `make vendor_rebuild` if vendoring has changed.
* [ ] Bump the default installed version of Go in `bootstrap-debian.sh` ([source](./bootstrap/bootstrap-debian.sh#L40-42)).
* [ ] Replace other mentions of the older version of go (grep for `golang:<old_version>` and `go<old_version>`).
* [ ] Update the `builder.dockerImage` parameter in the TeamCity [`Cockroach`](https://teamcity.cockroachdb.com/admin/editProject.html?projectId=Cockroach&tab=projectParams) and [`Internal`](https://teamcity.cockroachdb.com/admin/editProject.html?projectId=Internal&tab=projectParams) projects.

You can test the new builder image in TeamCity by using the custom parameters
UI (the "..." icon next to the "Run" button) to verify the image before
committing the change.

## Updating the nodejs version

Please follow the instructions above on updating the golang version, omitting the go-version-check.sh step.

#  Dependencies

Dependencies are managed using `go mod`. We use `go mod vendor` so that we can import and use non-Go files (e.g. protobuf files) using the [modvendor](https://github.com/goware/modvendor) script.

## Usage

### Installing or updating a dependency

Run `go get -u <dependency>`. To get a specific version, run `go get -u <dependency>@<version|branch|sha>`.

When updating a dependency, you should run `go mod tidy` after `go get` to ensure the old entries
are removed from go.sum.

You must then run `make vendor_rebuild` to ensure the modules are installed. These changes must
then be committed in the submodule directory (see [Working with Submodules](#working-with-submodules)).

Programs can then be run using `go build -mod=vendor ...` or `go test -mod=vendor ...`.

### Removing a dependency

When a dependency has been removed, run `go mod tidy` and then `make vendor_rebuild`.
Then follow the [Working with Submodules](#working-with-submodules) steps.

### Requiring a new tool

When installing a tool, you may need to add blank import to `pkg/cmd/import-tools/main.go` so that
`go mod tidy` does not clean it up.


## Working with Submodules

To keep the bloat of all the changes in all our dependencies out of our main
repository, we embed `vendor` as a git submodule, storing its content and
history in [`vendored`](https://github.com/cockroachdb/vendored) instead.

This split across two repositories however means that changes involving
changed dependencies require a two step process.

- After altering dependencies and making related code
changes, `git status` in `cockroachdb/cockroach` checkout will report that the
`vendor` submodule has `modified/untracked content`.

- Switch into `vendor` and commit all changes (or use `git -C vendor`), on a
new named branch.

   + At this point the `git status` in your `cockroachdb/cockroach` checkout
will report `new commits` for `vendor` instead of `modified content`.

- Commit your code changes and new `vendor` submodule ref.

- Before this commit can be submitted in a pull request to
`cockroachdb/cockroach`, the submodule commit it references must be available
on `github.com/cockroachdb/vendored`.

* Organization members can push their named branches there directly.

* Non-members should fork the `vendored` repo and submit a pull request to
`cockroachdb/vendored`, and need wait for it to merge before they will be able
to use it in a `cockroachdb/cockroach` PR.

### `master` Branch Pointer in Vendored Repo

Since the `cockroachdb/cockroach` submodule references individual commit
hashes in `vendored`, there is little significance to the `master` branch in
`vendored` -- as outlined above, new commits are always authored with the
previously referenced commit as their parent, regardless of what `master`
happens to be.

That said, it is critical that any ref in `vendored` that is referenced from
`cockroachdb/cockroach` remain available in `vendored` in perpetuity: after a
PR referencing a ref merges, the `vendored` `master` branch should be updated
to point to it before the named feature branch can be deleted, to ensure the
ref remains reachable and thus is never garbage collected.

### Conflicting Submodule Changes

The canonical linearization of history is always the main repo. In the event
of concurrent changes to `vendor`, the first should cause the second to see a
conflict on the `vendor` submodule pointer. When resolving that conflict, it
is important to re-run `go mod tidy `and `make vendor_rebuild`
against the fetched, updated `vendor` ref, thus generating a new commit in
the submodule that has as its parent the one from the earlier change.

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
