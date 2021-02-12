// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"sort"
	"strings"
)

func main() {
	// First list all tests.
	buf, err := exec.Command("bazel", "query", "kind(go_test, //pkg/...)", "--output=label").Output()
	if err != nil {
		log.Printf("Could not query Bazel tests: got error %v", err)
		os.Exit(1)
	}
	labels := strings.Split(string(buf[:]), "\n")
	sort.Slice(labels, func(i, j int) bool { return labels[i] < labels[j] })

	// Write the output to stdout
	fmt.Println("# GENERATED FILE DO NOT EDIT")
	fmt.Println("")
	fmt.Println("# gazelle:proto_strip_import_prefix /pkg")
	fmt.Println("")
	fmt.Println("ALL_TESTS = [")
	for _, label := range labels {
		if len(label) > 0 {
			fmt.Printf("    \"%s\",\n", label)
		}
	}
	fmt.Println("]")
	fmt.Println("")
	fmt.Println("# These suites run only the tests with the appropriate `size` (excepting those")
	fmt.Println("# tagged `broken_in_bazel`) [1]. Note that tests have a default timeout")
	fmt.Println("# depending on the size [2].")
	fmt.Println("")
	fmt.Println("# [1] https://docs.bazel.build/versions/master/be/general.html#test_suite")
	fmt.Println("# [2] https://docs.bazel.build/versions/master/be/common-definitions.html#common-attributes-tests")

	for _, size := range []string{"small", "medium", "large", "enormous"} {
		fmt.Println("")
		fmt.Println("test_suite(")
		fmt.Printf("    name = \"%s_tests\",\n", size)
		fmt.Println("    tags = [")
		fmt.Println("        \"-broken_in_bazel\",")
		fmt.Printf("        \"%s\",\n", size)
		fmt.Println("    ],")
		fmt.Println("    tests = ALL_TESTS,")
		fmt.Println(")")
	}
}
