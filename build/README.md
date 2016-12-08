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
A snapshot of cockroachdb's dependencies is maintained at https://github.com/cockroachdb/vendored
and checked out as a submodule at `./vendor`.

## Updating Dependencies
This snapshot was built and is managed using `glide`.

The [docs](https://github.com/Masterminds/glide) have detailed instructions, but in brief:
* run `./scripts/glide.sh` in `cockroachdb/cockroach`.
* add new dependencies with `./scripts/glide.sh get -s github.com/my/dependency`
	nb: glide uses `import` statements as the starting point when determining what to vendor.
	- if you are importing your new depenency in code, you should be fine.
	- if you are not (e.g. a binary tool), you need to add it to build/tool_imports.go to force an import.
* update dependencies to their latest version with `./scripts/glide.sh up`
  - to pin a dependency to a particular version, add a `version: ...` line to `glide.yaml`, then update.
  - to update a pinned dependency, change the version in `glide.yaml`, then update.

Updating a single dependency? glide does *not* expose a method to just update a single dependency.
Since glide attempts to consider the expressed version requirements for all transitive dependencies,
updating a single dependency could transitively change the resolution of others, and glide currently
chooses to always do a full resolution to address this.

If an attempt to update a dependency is pulling in unwanted/risky changes to another library, you
can and perhaps should pin the other library.

You can also, if you *really* want to, just change the resolution of a single dependency, by editing
its resolved version in glide.lock, then re-generating `vendor` from the edited resolution with
`sciprts/glide.sh install`. This is not recommended, as it circumvents the normal resolution logic.

## Working with Submodules
Since dependendies are stored in their own repository, and `cockroachdb/cockroach` depends on it,
changing a dependency requires pushing two things, in order: first the changes to the `vendored`
repository, then the reference to those changes to the main repository.

After adding or updating dependencies (and running all tests), switch into the `vendor` submodule
and commit changes (or use `git -C`). The commit must be available on `github.com/cockroachdb/vendored`
*before* submitting a pull request to `cockroachdb/cockroach` that references it.
* Organization members can push new refs directly.
  * Likely want to `git remote set-url origin git@github.com:cockroachdb/vendored.git`.
  * If not pushing to `master`, be sure to tag the ref so that it will not be GC'ed.
* Non-members should fork the `vendored` repo, add their fork as a second remote, and submit a pull
request.
  * After the pull request is merged, fetch and checkout the new `origin/master`, and commit *that* as the
  new ref in `cockroachdb/cockroach`.

## Repository Name
We only want the vendor directory used by builds when it is explicitly checked out *and managed* as a
submodule at `./vendor`.

If a go build fails to find a dependency in `./vendor`, it will continue searching anything named
"vendor" in parent directories. Thus the vendor repository is _not_ named "vendor", to minimize the risk
of it ending up somewhere in `GOPATH` with the name `vendor` (e.g. if it is manually cloned), where
it could end up being unintentionally used by builds and causing confusion.
