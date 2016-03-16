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
dependencies](#dependencies)

## Development
While actively developing the user interface, use `make debug` in order to run
go-bindata in debug mode.  This will instruct go-bindata to proxy your local
files (rather than using embedded versions) so you'll be able to edit them live
without recompiling or restarting the server.

For this change to be picked up, you'll need to run `make build` in the project root.

Note that only those files that were present the last time you ran `make debug`
will be proxied; if you add a new file, you must run recompile and restart the
server after running `make debug`.

Before committing, be sure to run `make` to generate a non-debug version of
`embedded.go`. This is enforced by our build system, but forgetting to do this
will result in wasted time waiting for the build.

## Watch/Livereload
If you want to automatically recompile/copy the typescript/stylus/index files,
you can use `make watch`. This runs [Gulp](http://gulpjs.com/) under the hood.

The website can also automatically pick up your changes with [LiveReload]
(http://livereload.com/) while `make watch` is running. The [Chrome LiveReload Plugin]
(https://chrome.google.com/webstore/detail/livereload/jnihajbhpnppcggbcgedagnkighmdlei?hl=en)
is an easy way to take advantage of this.

Note that if you add a new file, you'll need to restart `make watch` and run
`make build` in the project root again.

## Dependencies
Our admin UI is compiled using a collection of tools that depends on
[nodejs](https://nodejs.org/), so you'll want to have that installed.

We use npm to manage various dependencies; be sure that your node installation
includes a recent version of npm. If you observe problems with npm, try updating
it using `npm install -g npm`.

We use Bower to manage frontend dependencies and Typings to manage typescript
definition files.
Our Makefile automatically installs these tools locally, so for the most part,
you can be blissfully ignorant of their use. However, if you wish to add
Bower/Typings dependencies (and do not have your own opinions on binstubs), you'll
want to run them from the local install using one of:
- `node_modules/.bin/bower install --save <myAwesomeDep>`
- `node_modules/.bin/typings install --save <myAwesomeDep>`

To modify an existing npm dependency, you'll need to edit `package.json` in the
standard fashion, while to add a new npm dependency, you'll want to run:

```
	npm install --save <myAwesomeDep>
```

Either way, complete any npm changes by running:
```
	rm -r node_modules && npm update && node_modules/.bin/shonkwrap
```

Be sure to commit any changes to `npm-shrinkwrap.json`.

The `--save` modifier and `shonkwrap` invocation above are necessary to properly
lock down dependencies for other developers on the project, so make sure you don't
elide them!

If anything seems amiss with your dependencies, try running `make clean` from
within your `ui` directory.

## Testing your change

Before you submit your change for review, please run the following tests:

- [ ] Make your browser window really narrow, checking for strange wrapping of
  HTML elements and any other weird behavior.
- [ ] Make your browser window really wide, checking for undesired movement of
  HTML elements.
- If you're changing graphs on one page, when applicable, ensure that the same
  graph works in all of these places:
  - [ ] Cluster overview
  - [ ] Nodes page - Overview
  - [ ] Nodes page - Graph
  - [ ] Single-node graphs: go to Nodes page, click on a single node, and view
    its graphs

Checking your work with different browser window dimensions is especially
important, because we don't yet have a responsive admin UI design.

## Reviewing UI changes

We really should have a UI style guide when the UI code is more settled.

In addition to reviewing for correctness and consistent, idiomatic use of
TypeScript and the various UI frameworks we use, pay particular attention to
clarity. TypeScript, especially when combined with various functional
programming libraries, can be succinct at the expense of readability. If the
code can't be made clearer, check that there are sufficient comments.

If possible, walk through the changes with the pull request author interactively,
in-person or via Google Hangout. If that's not possible, pull their working
branch and test the changes yourself. A little interactive UI testing can go a
long way.