// Copyright 2022 The Cockroach Authors.
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
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/obsservice/lib"
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
		cfg := lib.ReverseHTTPProxyConfig{
			HTTPAddr:      httpAddr,
			TargetURL:     targetURL,
			CACertPath:    caCertPath,
			UICertPath:    uiCertPath,
			UICertKeyPath: uiCertKeyPath,
		}

		// Block forever running the proxy.
		<-lib.NewReverseHTTPProxy(ctx, cfg).RunAsync(ctx)
	},
}

// Flags.
var (
	httpAddr                  string
	targetURL                 string
	caCertPath                string
	uiCertPath, uiCertKeyPath string
)

func main() {
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

	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		exit.WithCode(exit.UnspecifiedError())
	}
}
