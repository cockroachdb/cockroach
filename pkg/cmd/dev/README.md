WARNING: `dev` isn't feature-complete, and the migration to Bazel at Cockroach
is still in-progress. Proceed at your own risk :)

Dev is a general-purpose dev tool for folks working on `cockroach`.

    $ dev -h
      <...>
      Usage:
        dev [command]

      Available Commands:
        bench       Run the specified benchmarks
        build       Build the specified binaries
        generate    Generate the specified files
        lint        Run the specified linters
        test        Run the specified tests

      Flags:
        -h, --help      help for dev
        -v, --version   version for dev

      Use "dev [command] --help" for more information about a command.

To update the testdata files:

    $ go test -run TestDatadriven -rewrite [-record] [-from-checkout=<path-to-crdb-checkout>]
