// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package main

import (
	"context"
	"flag"
	"html/template"
	"os"
	"path"

	"github.com/cockroachdb/cockroach/pkg/util/log"

	"github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl/upgradeinterlockccl/sharedtestutil"
)

var workspacePath = flag.String("workspace-path", "", "path to the root of the cockroach workspace")

func main() {
	flag.Parse()
	if *workspacePath == "" {
		log.Fatal(context.Background(), "You need to pass -workspace-path flag as the workspace root")
	}

	// testCases maps test name suffix to test file path.
	data := struct {
		Tests    map[string]sharedtestutil.TestConfig
		Variants map[sharedtestutil.TestVariant]string
	}{Tests: sharedtestutil.Tests, Variants: sharedtestutil.Variants}

	tmpl := template.Must(template.New("source").Parse(test_template))
	// pkg/ccl/kvccl/kvtenantccl/upgradeinterlockccl
	file, err := os.Create(path.Join(*workspacePath, "pkg", "ccl", "kvccl", "kvtenantccl", "upgradeinterlockccl", "generated_test.go"))
	if err != nil {
		log.Fatal(context.Background(), err.Error())
	}
	defer file.Close()
	if err := tmpl.Execute(file, data); err != nil {
		log.Fatal(context.Background(), err.Error())
	}
}
