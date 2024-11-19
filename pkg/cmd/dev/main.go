// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"log"
	"os"
)

const (
	beaverHubServerEndpoint = "https://beaver-hub-server-jjd2v2r2dq-uk.a.run.app/process"
	bepFileBasename         = "build_event_binary_file"
)

func main() {
	log.SetFlags(0)
	log.SetPrefix("")

	dev := makeDevCmd()

	if err := dev.cli.Execute(); err != nil {
		log.Printf("ERROR: %v", err)
		os.Exit(1)
	}
}
