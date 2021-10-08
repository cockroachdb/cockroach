// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// uptodate efficiently computes whether an output file is up-to-date with
// regard to its input files.
package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/MichaelTJones/walk"
	"github.com/cockroachdb/errors/oserror"
	"github.com/spf13/pflag"
)

var debug = pflag.BoolP("debug", "d", false, "debug mode")

func die(err error) {
	fmt.Fprintf(os.Stderr, "%s: %s\n", os.Args[0], err)
	os.Exit(2)
}

func main() {
	pflag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [-d] OUTPUT INPUT...\n", os.Args[0])
	}
	pflag.Parse()
	if pflag.NArg() < 2 {
		pflag.Usage()
		os.Exit(1)
	}
	if !*debug {
		log.SetOutput(ioutil.Discard)
	}
	output, inputs := pflag.Arg(0), pflag.Args()[1:]

	fi, err := os.Stat(output)
	if oserror.IsNotExist(err) {
		log.Printf("output %q is missing", output)
		os.Exit(1)
	} else if err != nil {
		die(err)
	}
	outputModTime := fi.ModTime()

	for _, input := range inputs {
		err = walk.Walk(input, func(path string, fi os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if fi.IsDir() {
				return nil
			}
			if !fi.ModTime().Before(outputModTime) {
				log.Printf("input %q (mtime %s) not older than output %q (mtime %s)",
					path, fi.ModTime(), output, outputModTime)
				os.Exit(1)
			}
			return nil
		})
		if err != nil {
			die(err)
		}
	}
	log.Printf("all inputs older than output %q (mtime %s)\n", output, outputModTime)
	os.Exit(0)
}
