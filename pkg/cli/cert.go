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
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/pkg/errors"
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
	RunE:         maybeDecorateGRPCError(runCreateCACert),
}

// runCreateCACert generates key pair and CA certificate and writes them
// to their corresponding files.
func runCreateCACert(cmd *cobra.Command, args []string) error {
	if len(baseCfg.SSLCA) == 0 || len(baseCfg.SSLCAKey) == 0 {
		return errMissingParams
	}
	return errors.Wrap(security.RunCreateCACert(baseCfg.SSLCA, baseCfg.SSLCAKey, keySize), "failed to generate CA certificate")
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
	RunE:         maybeDecorateGRPCError(runCreateNodeCert),
}

// runCreateNodeCert generates key pair and CA certificate and writes them
// to their corresponding files.
func runCreateNodeCert(cmd *cobra.Command, args []string) error {
	if len(baseCfg.SSLCA) == 0 || len(baseCfg.SSLCAKey) == 0 ||
		len(baseCfg.SSLCert) == 0 || len(baseCfg.SSLCertKey) == 0 {
		return errMissingParams
	}
	return errors.Wrap(security.RunCreateNodeCert(baseCfg.SSLCA, baseCfg.SSLCAKey,
		baseCfg.SSLCert, baseCfg.SSLCertKey, keySize, args),
		"failed to generate node certificate",
	)
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
	RunE:         maybeDecorateGRPCError(runCreateClientCert),
}

// runCreateClientCert generates key pair and CA certificate and writes them
// to their corresponding files.
func runCreateClientCert(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return usageAndError(cmd)
	}
	if len(baseCfg.SSLCA) == 0 || len(baseCfg.SSLCAKey) == 0 ||
		len(baseCfg.SSLCert) == 0 || len(baseCfg.SSLCertKey) == 0 {
		return errMissingParams
	}

	return errors.Wrap(security.RunCreateClientCert(baseCfg.SSLCA, baseCfg.SSLCAKey,
		baseCfg.SSLCert, baseCfg.SSLCertKey, keySize, args[0]),
		"failed to generate clent certificate",
	)
}

var certCmds = []*cobra.Command{
	createCACertCmd,
	createNodeCertCmd,
	createClientCertCmd,
}

var certCmd = &cobra.Command{
	Use:   "cert",
	Short: "create ca, node, and client certs",
	RunE: func(cmd *cobra.Command, args []string) error {
		return cmd.Usage()
	},
}

func init() {
	certCmd.AddCommand(certCmds...)
}
