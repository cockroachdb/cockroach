// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package cli

import (
	"fmt"

	"github.com/cockroachdb/cockroach/security"

	"github.com/spf13/cobra"
)

const defaultKeySize = 2048

var keySize int

// A createCACert command generates a CA certificate and stores it
// in the cert directory.
var createCACertCmd = &cobra.Command{
	Use:   "create-ca --ca-cert=<path-to-ca-cert> --ca-key=<path-to-ca-key>",
	Short: "create CA cert and key",
	Long: `
Generates CA certificate and key, writing them to --ca-cert and --ca-key.
`,
	SilenceUsage: true,
	RunE:         runCreateCACert,
}

// runCreateCACert generates key pair and CA certificate and writes them
// to their corresponding files.
func runCreateCACert(cmd *cobra.Command, args []string) error {
	if len(cliContext.SSLCA) == 0 || len(cliContext.SSLCAKey) == 0 {
		mustUsage(cmd)
		return errMissingParams
	}
	if err := security.RunCreateCACert(cliContext.SSLCA, cliContext.SSLCAKey, keySize); err != nil {
		return fmt.Errorf("failed to generate CA certificate: %s", err)
	}
	return nil
}

// A createNodeCert command generates a node certificate and stores it
// in the cert directory.
var createNodeCertCmd = &cobra.Command{
	Use:   "create-node --ca-cert=<ca-cert> --ca-key=<ca-key> --cert=<node-cert> --key=<node-key> <host 1> <host 2> ... <host N>",
	Short: "create node cert and key",
	Long: `
Generates node certificate and keys for a given node, writing them to
--cert and --key. CA certificate and key must be passed in.
At least one host should be passed in (either IP address or dns name).
`,
	SilenceUsage: true,
	RunE:         runCreateNodeCert,
}

// runCreateNodeCert generates key pair and CA certificate and writes them
// to their corresponding files.
func runCreateNodeCert(cmd *cobra.Command, args []string) error {
	if len(cliContext.SSLCA) == 0 || len(cliContext.SSLCAKey) == 0 ||
		len(cliContext.SSLCert) == 0 || len(cliContext.SSLCertKey) == 0 {
		mustUsage(cmd)
		return errMissingParams
	}
	if err := security.RunCreateNodeCert(cliContext.SSLCA, cliContext.SSLCAKey,
		cliContext.SSLCert, cliContext.SSLCertKey, keySize, args); err != nil {
		return fmt.Errorf("failed to generate node certificate: %s", err)
	}
	return nil
}

// A createClientCert command generates a client certificate and stores it
// in the cert directory under <username>.crt and key under <username>.key.
var createClientCertCmd = &cobra.Command{
	Use:   "create-client --ca-cert=<ca-cert> --ca-key=<ca-key> --cert=<node-cert> --key=<node-key> username",
	Short: "create client cert and key",
	Long: `
Generates a client certificate and key, writing them to --cert and --key.
--cert and --key. CA certificate and key must be passed in.
The certs directory should contain a CA cert and key.
`,
	SilenceUsage: true,
	RunE:         runCreateClientCert,
}

// runCreateClientCert generates key pair and CA certificate and writes them
// to their corresponding files.
func runCreateClientCert(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		mustUsage(cmd)
		return errMissingParams
	}
	if len(cliContext.SSLCA) == 0 || len(cliContext.SSLCAKey) == 0 ||
		len(cliContext.SSLCert) == 0 || len(cliContext.SSLCertKey) == 0 {
		mustUsage(cmd)
		return errMissingParams
	}

	if err := security.RunCreateClientCert(cliContext.SSLCA, cliContext.SSLCAKey,
		cliContext.SSLCert, cliContext.SSLCertKey, keySize, args[0]); err != nil {
		return fmt.Errorf("failed to generate clent certificate: %s", err)
	}
	return nil
}

var certCmds = []*cobra.Command{
	createCACertCmd,
	createNodeCertCmd,
	createClientCertCmd,
}

var certCmd = &cobra.Command{
	Use:   "cert",
	Short: "create ca, node, and client certs",
	Run: func(cmd *cobra.Command, args []string) {
		mustUsage(cmd)
	},
}

func init() {
	certCmd.AddCommand(certCmds...)
}
