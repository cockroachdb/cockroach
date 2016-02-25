// Package gotool is a library of utility functions used to implement the standard "Go" tool provided
// as a convenience to developers who want to write tools with similar semantics.
package gotool

// export functions as here to make it easier to keep the implementations up to date with upstream.

// ImportPaths returns the import paths to use for the given arguments using default context.
//
// The path "all" is expanded to all packages in $GOPATH and $GOROOT.
// The path "std" is expanded to all packages in the Go standard library.
// The string "..." is treated as a wildcard within a path.
// Relative import paths are not converted to full import paths.
func ImportPaths(args []string) []string {
	return DefaultContext.ImportPaths(args)
}
