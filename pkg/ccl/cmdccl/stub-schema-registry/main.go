// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

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
