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

### Deployment

The deploy image is a downsized image containing a minimal environment for
running CockroachDB. It is based on Debian Jessie and contains only the main
CockroachDB binary. To fetch this image, run `docker pull
cockroachdb/cockroach` in the usual fashion.

To build the image yourself, use `./build-docker-deploy.sh`. The script will
build and run a development container. The CockroachDB binary will be built
inside of that container. That binary is built into our minimal container. The
resulting image `cockroachdb/cockroach` can be run via `docker run` in the
usual fashion.

#  Dependencies

A snapshot of CockroachDB's dependencies is maintained at https://github.com/cockroachdb/vendored
and checked out as a sub-module at `./vendor`.

## Updating Dependencies

This snapshot was built and is managed using `glide`.

### Installing Glide

Get or update glide with `go get -u github.com/Masterminds/glide`
- Versions in `brew` or elsehwere are sometimes missing recent fixes.
- If installing from master becomes unstable, you can use the oldest version that correctly handles
submodules (`ab0972eb`), e.g.
```
git -C $GOPATH/src/github.com/Masterminds/glide checkout ab0972eb && \
go install github.com/Masterminds/glide
```

### Using Glide

`glide` uses import statements in our code to discover what it needs to vendor.

- When introducing a new library, adding an import and running `glide up` will fetch it to `vendor`.
  - Don't try to use `glide get`, since it [will delete all comments](Masterminds/glide/issues/691) in `glide.yaml`.
- If you are adding a non-import dependency (e.g. a binary tool to be used in development),
  please add a dummy import to `build/tool_imports.go` to ensure glide remains [aware of it](Masterminds/glide/issues/690).
- [glide-diff-parser](cockroachdb/glide-diff-parser) can be useful to inspect or summarize changes.

### Version Pins

We pin many of our dependencies in `glide.yaml` to make it easier to update or add a single dependency.

Glide always re-resolves everything when updating any dependency (to preserve correctness given
potential changes in transitive requirements). Unfortunately it always picks the latest version of a
dependency unless it has a direct or transitive pin, which means even when attempting to `update` or
`get` even just one dependency, any other unreleated dependencies could unexpectedly change versions
unless they are pinned.

Thus for libraries where we care about what version we resolve -- e.g. if they affect stability, if we
want to vet upstream chanages, if we rely on features or fixes in specific upstream versions, etc --
we pin revisions to make them stable between `update` runs, and only unpin them when we actually want
them to change. While not a hard rule, if we directly import something in our code, there's a decent
chance we care about it enough that we want its version to remain stable unless intentionally changed,
and thus it may benefit from being pinned.

In cases where a sweeping update of all deps is actually desired, comment out all the stability pins
section of `glide.yaml`, run `update`, then restore the pins with their new revisions.

### Working with Sub-modules

To keep the bloat of all the changes in all our dependencies out of our main repository, we embed
`vendor` as a git submodule, storing its content and history in [`vendored`](cockroachdb/vendored) instead.

This split across two repositories however means that changes involving changed dependencies require
a two step process.

- After using glide as described above to add or update dependencies and making related code changes,
`git status` in `cockroachdb/cockroach` checkout will report that the `vendor` sub-module has
`modified/untracked content`
- Switch into `vendor` and commit all changes (or use `git -C vendor`), on a new named branch.
  - At this point the `git status` in your `cockroachdb/cockroach` checkout will report
  `new commits` for `vendor` instead of `modified content`.
- Commit your code changes and new `vendor` submodule ref.
- Before this commit can be submitted in a pull request to `cockroachdb/cockroach`, the submodule
commit it references must be available on `github.com/cockroachdb/vendored`.
* Organization members can push their named branches there directly.
* Non-members should fork the `vendored` repo and submit a pull request to `cockroachdb/vendored`,
  and need wait for it to merge before they will be able to use it in a `cockroachdb/cockroach` PR.

#### `master` Branch Pointer in Vendor Repo

Since `cockroachdb/cockroach` references individual commit hashes in `vendored`, there is little
significance given to the `master` branch in `vendored`. That said, we never want a `vendored`
commit that is referenced in `cockroachdb/cockroach` to be garbage collected, so if you opt to
delete your named branch from `vendored` after your PR merges, you should update the `master` branch
pointer to ensure your commit is forever reachable.

#### Conflicting Submodule Changes

The canonical linearization of history is always the main repo. In the event of concurrent
changes to `vendor`, the first should cause the second to see a conflict on the `vendor` submodule
pointer. When resolving that conflict, it is important to re-run glide against the fetched, updated
`vendor` ref, thus generating a new commit in the submodule that has as its parent the one from the
earlier change.

## Repository Name

We only want the vendor directory used by builds when it is explicitly checked out *and managed* as a
sub-module at `./vendor`.

If a go build fails to find a dependency in `./vendor`, it will continue searching anything named
"vendor" in parent directories. Thus the vendor repository is _not_ named "vendor", to minimize the risk
of it ending up somewhere in `GOPATH` with the name `vendor` (e.g. if it is manually cloned), where
it could end up being unintentionally used by builds and causing confusion.
