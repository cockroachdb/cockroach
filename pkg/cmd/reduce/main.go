// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// reduce reduces SQL passed over stdin using cockroach demo. The input is
// simplified such that the contains argument is present as an error during SQL
// execution.
package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/testutils/reduce"
	"github.com/cockroachdb/cockroach/pkg/testutils/reduce/reducesql"
)

var (
	flags    = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	path     = flags.String("path", "./cockroach", "path to cockroach binary")
	verbose  = flags.Bool("v", false, "log progress")
	contains = flags.String("contains", "", "error regex to search for")
)

func usage() {
	fmt.Fprintf(flags.Output(), "Usage of %s:\n", os.Args[0])
	flags.PrintDefaults()
	os.Exit(1)
}

func main() {
	if err := flags.Parse(os.Args[1:]); err != nil {
		usage()
	}
	if *contains == "" {
		fmt.Print("missing contains\n\n")
		usage()
	}
	out, err := reduceSQL(*path, *contains, *verbose)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(out)
}

func reduceSQL(path, contains string, verbose bool) (string, error) {
	containsRE, err := regexp.Compile(contains)
	if err != nil {
		return "", err
	}
	input, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		return "", err
	}

	// Pretty print the input so the file size comparison is useful.
	inputSQL, err := reducesql.Pretty(input)
	if err != nil {
		return "", err
	}

	var logger io.Writer
	if verbose {
		logger = os.Stderr
		fmt.Fprintf(logger, "input SQL pretty printed, %d bytes -> %d bytes\n", len(input), len(inputSQL))
	}

	interesting := func(f reduce.File) bool {
		cmd := exec.Command(path, "demo")
		sql := string(f)
		if !strings.HasSuffix(sql, ";") {
			sql += ";"
		}
		cmd.Stdin = strings.NewReader(sql)
		out, err := cmd.CombinedOutput()
		switch err := err.(type) {
		case *exec.Error:
			if err.Err == exec.ErrNotFound {
				log.Fatal(err)
			}
		case *os.PathError:
			log.Fatal(err)
		}
		return containsRE.Match(out)
	}

	out, err := reduce.Reduce(logger, reduce.File(inputSQL), interesting, reducesql.SQLPasses...)
	return string(out), err
}
