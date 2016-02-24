Glock is a command-line tool to lock dependencies to specific revisions, using a
version control hook to keep those revisions in sync across a team.

## Overview

Glock provides 2 commands and a version control hook:

* "glock save project" writes the transitive repo root[1] dependencies of all packages under "project/..." to a GLOCKFILE
* "glock sync project" updates all packages listed in project/GLOCKFILE to the listed version.
* "glock install project" installs a version control hook that watches for changes to project/GLOCKFILE and incrementally applies them.

GLOCKFILEs are simple text files that record a repo roots's revision, e.g.

```
bitbucket.org/tebeka/selenium 02df1758050f
code.google.com/p/cascadia 4f03c71bc42b
code.google.com/p/go-uuid 7dda39b2e7d5
...
```

[1] "repo root" refers to the base package in a repository.  For example, although code.google.com/p/go.net/websocket is a Go package, code.google.com/p/go.net is the "repo root", and any dependencies on non-root packages roll up to the root.

## Use case

It is meant to serve a team that:

* develops multiple applications within a single Go codebase
* uses a single dedicated GOPATH for development
* wants all applications within the codebase to use one version of any dependency.

For example, at work we keep our Go code in one repo (rather than many small ones) and use a single GOPATH.  This tool allows us to gain reproducible builds, with version updates automatically propagated to the team via the hook, with the following advantages:

* We still use the normal Go toolchain / dev process (e.g. not having to run everything in a godep sandbox).  We can more easily contribute to 3rd party libraries, since they are not in a vendor sandbox or have rewritten import paths.
* We avoid the repo bloat of checking in our dependencies (> 100 MB), in addition to the extra churn.  Updating a dependency involves a change to one line of a text file instead of thousand-line diffs.
* Much easier and less error-prone than manually checking in dependencies.  Developers don't have to fight git because git wants to make the project a submodule instead of just checking in the files.  Running glock on your CI server or as a pre-commit hook can ensure that any new dependencies have been recorded.

## Setup

Here is how to get started with Glock.

```
# Fetch and install glock
$ go get github.com/robfig/glock

# Record the package's transitive dependencies, as they currently exist.
# Glock writes the dependencies to a GLOCKFILE in that package's directory.
# All dependencies of all descendent packages are included.
$ glock save github.com/acme/project

# Review and check in the dependencies.
$ git add src/github.com/acme/project/GLOCKFILE
$ git commit -m 'Save current dependency revisions'
$ git push

# All developers install the git hook
$ glock install github.com/acme/project
```

Once the VCS hook is installed, all developers will have their dependencies
added and updated automatically as the GLOCKFILE changes.

## Add/update a dependency

```
# Developer wants to add a dependency
$ go get -u github.com/some/dependency
$ glock save github.com/acme/project
$ git commit src/github.com/acme/project/GLOCKFILE
$ git push
```

"go get -u" will download the latest revision of that library and update to it.  "glock save" records the current state of dependencies in your GOPATH, which should reflect the new or updated revision.

You can use the same process to update all dependencies to the latest revision:
```
$ cd $GOPATH/src
$ go get -u -v ./...
$ glock save github.com/acme/project
...
```

In any case, the dependency update will be propagated to all team members as they pull that
revision.

## Continuous Integration

It may also be useful to verify that all dependencies are recorded as part of your continuous build.  A simple diff works:

```
$ diff <(glock save -n github.com/acme/project) <(cat github.com/acme/project/GLOCKFILE)
```

That will return success (0) if there were no differences between the current project dependencies and what is recorded in the GLOCKFILE, or it will exit with an error (1) and print the differences.

## Commands

Glock can also be used to build and update go programs across the team.

Commands are declared at the top of your GLOCKFILE:

```
cmd code.google.com/p/go.tools/cmd/godoc
cmd code.google.com/p/go.tools/cmd/goimports
cmd code.google.com/p/go.tools/cmd/vet
...
```

The declarations have a couple effects:

* These commands will have their dependencies included when glock calculates them.
* The commands will be built (if necessary) during "glock sync".
* New commands will be installed by "glock apply", and existing commands will be
  re-installed when package versions are updated.
