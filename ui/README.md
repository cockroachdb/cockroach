# Embedded UI

This directory contains the client-side code for cockroach's web admin console.
These files are embedded into the cockroach binary via the
[go-bindata](https://github.com/jteeuwen/go-bindata) package, which is used to
generate the `embedded.go` file in this directory.

## Modification
If any modifications are made to the contents of this directory, run `make`
in this directory to generate a new `embedded.go`, which should _then be
committed along with the original files_. The generated file is committed
because making our web admin resource requires [additional non-go
dependencies](####dependencies)

## Development
While actively developing the user interface, use `make debug` in order to run
go-bindata in debug mode.  This will instruct go-bindata to proxy your local
files (rather than using embedded versions) so you'll be able to edit them live
without recompiling or restarting the server.

Note that only those files that were present the last time you ran `make debug`
will be proxied; if you add a new file, you must run recompile and restart the
server after running `make debug`.

Before committing, be sure to run `make` to generate a non-debug version of
`embedded.go`. This is enforced by our build system, but forgetting to do this
will result in wasted time waiting for the build.

## Dependencies
Our admin UI is written in Typescript, so compiling it depends on having the
typescript compiler locally. We also require a typescript linter (tslint).

1. Install [nodejs](https://nodejs.org/)
2. Install [typescript](http://www.typescriptlang.org/) and
   [tslint](https://github.com/palantir/tslint). This can be done via the
   command line using npm: `npm install -g typescript tslint`
