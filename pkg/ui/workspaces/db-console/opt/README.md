# ui/opt

This directory is a home for UI dependencies that are not essential to building
a working UI or running the UI tests. Since all contributors are required to
build the UI from scratch, limiting the number of dependencies in the pipeline,
especially heavyweight dependencies, minimizes the burden of the UI build on
non-UI developers. Make will run `yarn install` in this directory only when
necessary.

A rough rule of thumb for whether a UI dependency is essential is to ask whether
`make generate` or `make test` from within the UI requires the dependency. At
the time of writing, the only package that fails this test is
[webpack-dashboard], which displays Webpack build status in a handy terminal
user interface, complete with a progress bar. Though this might be useful to
non-UI developers, it depends on a package that compiles SQLite from source,
which is rather a lot to ask.

If the UI dependencies balloon, we can further separate the dependencies
required to run `make test` from the dependencies required to run `make
generate`. Each stratum adds build system complexity and duplicates shared
transitive dependencies, so we're only separating into essential and
non-essential dependencies to start.

[webpack-dashboard]: https://github.com/FormidableLabs/webpack-dashboard
