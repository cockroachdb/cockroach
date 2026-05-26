# CRDB fork of pkg/json [![GoDoc](https://godoc.org/github.com/pkg/json?status.svg)](https://godoc.org/github.com/pkg/json)

These files are a fork of `pkg/json` package.
- `scanner.go`, `decoder.go`, and `reader.go` were copied from pkg/json
- Additional utility methods added (e.g. to reset decoder)
- Ability to query reader position, etc.
- Package name renamed from `json` to `tokenizer`
- Removal of unused functions
- Probably, the largest change from the original is in the `scanner.go`.
  Changes were made to support un-escaping escaped characters, including
  unicode characters. 
