// Copyright 2022 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/httpproxy"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/migrations"
	_ "github.com/cockroachdb/cockroach/pkg/ui/distoss" // web UI init hooks
	"github.com/spf13/cobra"
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "obsservice",
	Short: "An observability service for CockroachDB",
	Long: `The Observability Service ingests monitoring and observability data 
from one or more CockroachDB clusters.`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		cfg := httpproxy.ReverseHTTPProxyConfig{
			HTTPAddr:      httpAddr,
			TargetURL:     targetURL,
			CACertPath:    caCertPath,
			UICertPath:    uiCertPath,
			UICertKeyPath: uiCertKeyPath,
		}

		if err := migrations.RunDBMigrations(ctx, sinkPGURL); err != nil {
			panic(err)
		}

		// Block forever running the proxy.
		<-httpproxy.NewReverseHTTPProxy(ctx, cfg).RunAsync(ctx)
	},
}

// Flags.
var (
	httpAddr                  string
	targetURL                 string
	caCertPath                string
	uiCertPath, uiCertKeyPath string
	sinkPGURL                 string
)

func main() {

	// Add all the flags registered with the standard "flag" package. Useful for
	// --vmodule, for example.
	RootCmd.PersistentFlags().AddGoFlagSet(flag.CommandLine)

	RootCmd.PersistentFlags().StringVar(
		&httpAddr,
		"http-addr",
		"localhost:8081",
		"The address on which to listen for HTTP requests.")
	RootCmd.PersistentFlags().StringVar(
		&targetURL,
		"crdb-http-url",
		"http://localhost:8080",
		"The base URL to which HTTP requests are proxied.")
	RootCmd.PersistentFlags().StringVar(
		&caCertPath,
		"ca-cert",
		"",
		"Path to the certificate authority certificate file. If specified,"+
			" HTTP requests are only proxied to CRDB nodes that present certificates signed by this CA."+
			" If not specified, the system's CA list is used.")
	RootCmd.PersistentFlags().StringVar(
		&uiCertPath,
		"ui-cert",
		"",
		"Path to the certificate used used by the Observability Service.")
	RootCmd.PersistentFlags().StringVar(
		&uiCertKeyPath,
		"ui-cert-key",
		"",
		"Path to the private key used by the Observability Service. "+
			"This is the key corresponding to the --ui-cert certificate.")

	// Flags about connecting to the sink cluster.
	RootCmd.PersistentFlags().StringVar(
		&sinkPGURL,
		"sink-pgurl",
		"postgresql://root@localhost:26257/defaultdb?sslmode=disable",
		"PGURL for the sink cluster.")

	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		exit.WithCode(exit.UnspecifiedError())
	}
}
