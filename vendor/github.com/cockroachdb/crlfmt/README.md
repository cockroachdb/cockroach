# crlfmt

`crlfmt` is a `gofmt`-style linter for Go code that enforces the CockroachDB Style Guide found [here](https://wiki.crdb.io/wiki/spaces/CRDB/pages/181371303/Go+coding+guidelines).

## Usage

```
$ go get github.com/cockroachdb/crlfmt
$ crlfmt [flags] <file path>

Flags:
  -diff             print diffs (default true)
  -fast             skip running goimports and simplify
  -groupimports     group imports by type (default true)
  -ignore <string>  regex matching files to skip
  -tab <int>        tab width for column calculations (default 2)
  -w                overwrite modified files
  -wrap <int>       column to wrap at (default 100)
```

## Examples

If you are running `crlfmt` on the http://github.com/cockroachdb/cockroach codebase, you can use the following command to reformat all files in the current directory, ignoring generated code files:

```
$ crlfmt -w -ignore '\.(pb(\.gw)?)|(\.[eo]g)\.go|/testdata/|^sql/parser/sql\.go$|_generated\.go$' .
```
