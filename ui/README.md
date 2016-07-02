# Embedded UI

This directory contains the client-side code for cockroach's web admin
console. These files are embedded into the cockroach binary via the
[go-bindata](https://github.com/jteeuwen/go-bindata) package, which is used to
generate the `embedded.go` file in this directory.

## Modification

If any modifications are made to the contents of this directory, run `make` in
this directory to generate a new `embedded.go`, which should _then be
committed along with the original files_. The generated file is committed
because compiling our web admin resource requires
[additional non-godependencies](#dependencies).

## Development

While actively developing the user interface, be sure to run the CockroachDB
server with the environment variable `COCKROACH_DEBUG_UI` set to a truthy
value, e.g. `COCKROACH_DEBUG_UI=1`, or `COCKROACH_DEBUG_UI=true`. This will
cause the CockroachDB server to load its UI assets directly from the disk,
rather than use the compiled-in assets.

In order to regenerate the on-disk assets, be sure to run `make` in the `ui`
directory. This will regenerate `build/app.{css,js}` as well as embedded.go.

For faster iteration still, you may want to point your browser at debug.html
rather than index.html at the admin UI's top level; this doesn't require
running `make` at all - instead, all UI sources will be loaded from disk and
compiled in the browser.

NOTE: styles are not yet compiled in the browser. As a workaround, `make
watch` is available; it automatically watches for style changes and recompiles
them, though a browser reload is still required. Note that if you add a new
file, you'll need to restart `make watch`.

Regardless of the workflow you choose, always be sure to run `make` in the
`ui` directory before committing, so that your commit includes the updated
`embedded.go`. This is enforced by our build system, but forgetting to do this
will result in wasted time waiting for the build.

## Dependencies
Our admin UI is compiled using a collection of tools that depends on
[Node.js](https://nodejs.org/), so you'll want to have that installed.

We use [npm](https://www.npmjs.com/) to manage various dependencies; be sure
that your Node.js installation includes a recent version of npm. If you
observe problems with npm, try updating it using `npm install -g npm`.

To modify an existing npm dependency, you'll need to edit `package.json` in
the standard fashion, while to add a new npm dependency, you'll want to run
`npm install --save <myAwesomeDep>`.

Either way, complete any npm changes by running:
```
rm -rf node_modules npm-shrinkwrap.json && npm update --no-progress && $(npm bin)/shonkwrap && $(npm bin)/jspm update
```

We use [JSPM](http://jspm.io/) to manage frontend dependencies and
[Typings](https://github.com/typings/typings) to manage typescript definition
files. Our Makefile automatically installs these tools locally, so for the
most part, you can be blissfully ignorant of their use. However, if you wish
to add JSPM/Typings dependencies (and do not have your own opinions on
binstubs), you'll want to run them from the local install using one of:

- `$(npm bin)/jspm install --save <myAwesomeDep>`
- `$(npm bin)/typings install --save <myAwesomeDep>`

Be sure to commit any changes resulting from your dependency changes.

The `--save` modifiers and `shonkwrap` invocation above are necessary to
properly lock down dependencies for other developers on the project, so make
sure you don't elide them!
