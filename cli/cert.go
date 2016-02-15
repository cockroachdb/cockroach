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
	Use:   "create-ca [options]",
	Short: "create CA cert and key",
	Long: `
Generates a new key pair and CA certificate, writing them to
individual files in the directory specified by --certs (required).
`,
	SilenceUsage: true,
	RunE:         runCreateCACert,
}

// runCreateCACert generates key pair and CA certificate and writes them
// to their corresponding files.
func runCreateCACert(cmd *cobra.Command, args []string) error {
	if err := security.RunCreateCACert(cliContext.Certs, keySize); err != nil {
		return fmt.Errorf("failed to generate CA certificate: %s", err)
	}
	return nil
}

// A createNodeCert command generates a node certificate and stores it
// in the cert directory.
var createNodeCertCmd = &cobra.Command{
	Use:   "create-node [options] <host 1> <host 2> ... <host N>",
	Short: "create node cert and key",
	Long: `
Generates server and client certificates and keys for a given node, writing them to
individual files in the directory specified by --certs (required).
The certs directory should contain a CA cert and key.
At least one host should be passed in (either IP address of dns name).
`,
	SilenceUsage: true,
	RunE:         runCreateNodeCert,
}

// runCreateNodeCert generates key pair and CA certificate and writes them
// to their corresponding files.
func runCreateNodeCert(cmd *cobra.Command, args []string) error {
	if err := security.RunCreateNodeCert(cliContext.Certs, keySize, args); err != nil {
		return fmt.Errorf("failed to generate node certificate: %s", err)
	}
	return nil
}

// A createClientCert command generates a client certificate and stores it
// in the cert directory under <username>.crt and key under <username>.key.
var createClientCertCmd = &cobra.Command{
	Use:   "create-client [options] username",
	Short: "create client cert and key",
	Long: `
Generates a new key pair and client certificate, writing them to
individual files in the directory specified by --certs (required).
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
	if err := security.RunCreateClientCert(cliContext.Certs, keySize, args[0]); err != nil {
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
