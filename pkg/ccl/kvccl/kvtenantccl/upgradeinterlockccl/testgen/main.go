// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"flag"
	"html/template"
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl/upgradeinterlockccl/sharedtestutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

var outputPath = flag.String("output-file-path", "", "path to the output file")

func main() {
	flag.Parse()
	if *outputPath == "" {
		log.Fatal(context.Background(), "You need to pass -output-file-path flag")
	}

	data := struct {
		Tests    map[string]sharedtestutil.TestConfig
		Variants map[sharedtestutil.TestVariant]string
	}{Tests: sharedtestutil.Tests, Variants: sharedtestutil.Variants}

	tmpl := template.Must(template.New("source").Parse(test_template))
	file, err := os.Create(filepath.Join(*outputPath))
	if err != nil {
		log.Fatalf(context.Background(), "failed to create file: %v", err)
	}
	defer file.Close()
	if err := tmpl.Execute(file, data); err != nil {
		log.Fatalf(context.Background(), "failed to execute template: %v", err)
	}
}
