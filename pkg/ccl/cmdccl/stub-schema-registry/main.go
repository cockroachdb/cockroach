// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
)

func main() {
	reg := cdctest.StartTestSchemaRegistry()
	defer reg.Close()

	fmt.Printf("Stub Schema Registry listening at %s\n", reg.URL())

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	sig := <-c
	fmt.Printf("Shutting down on signal: %s\n", sig)
}
