// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

// InitRoachprod initializes the roachprod providers by calling InitProviders.
// This function sets up the environment for running roachprod commands.
func InitRoachprod() {
	_ = roachprod.InitProviders()
}

// RoachprodRun runs a command on a roachprod cluster with the given cluster name and logger.
// It takes a list of command arguments and passes them to the roachprod command execution.
func RoachprodRun(clusterName string, l *logger.Logger, cmdArray []string) error {
	// Execute the roachprod command with the provided context, logger, cluster name, and options.
	return roachprod.Run(
		context.Background(), l, clusterName, "", "", false,
		os.Stdout, os.Stderr, cmdArray, install.DefaultRunOptions(),
	)
}

// InitLogger initializes and returns a logger based on the provided log file path.
// If the logger configuration fails, the program prints an error and exits.
func InitLogger(path string) *logger.Logger {
	loggerCfg := logger.Config{Stdout: os.Stdout, Stderr: os.Stderr} // Create a logger config with standard output and error.
	var loggerError error
	l, loggerError := loggerCfg.NewLogger(path) // Create a new logger based on the configuration.
	if loggerError != nil {
		// If there is an error initializing the logger, print the error message and exit the program.
		_, _ = fmt.Fprintf(os.Stderr, "unable to configure logger: %s\n", loggerError)
		os.Exit(1)
	}
	return l // Return the initialized logger.
}
