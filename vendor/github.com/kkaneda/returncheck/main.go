package main

import (
	"os"

	"github.com/kkaneda/returncheck/internal"
	"github.com/kisielk/gotool"
)

func main() {
	// TODO(kaneda): Add a commandline flag for specifying a target type.
	targetPkg := "github.com/cockroachdb/cockroach/roachpb"
	targetTypeName := "Error"
	if err := returncheck.Run(gotool.ImportPaths(os.Args[1:]), targetPkg, targetTypeName); err != nil {
		os.Exit(1)
	}
}
