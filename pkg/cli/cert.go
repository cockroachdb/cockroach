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
	"os"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/olekukonko/tablewriter"
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
	RunE: MaybeDecorateGRPCError(runCreateCACert),
}

// runCreateCACert generates key pair and CA certificate and writes them
// to their corresponding files.
func runCreateCACert(cmd *cobra.Command, args []string) error {
	// TODO(mberhault): fix
	/*	if len(baseCfg.SSLCA) == 0 || len(baseCfg.SSLCAKey) == 0 {
			return errMissingParams
		}
		return errors.Wrap(security.RunCreateCACert(baseCfg.SSLCA, baseCfg.SSLCAKey, keySize), "failed to generate CA certificate")
	*/
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
	RunE: MaybeDecorateGRPCError(runCreateNodeCert),
}

// runCreateNodeCert generates key pair and CA certificate and writes them
// to their corresponding files.
func runCreateNodeCert(cmd *cobra.Command, args []string) error {
	// TODO(mberhaul): fix
	/*
		if len(baseCfg.SSLCA) == 0 || len(baseCfg.SSLCAKey) == 0 ||
			len(baseCfg.SSLCert) == 0 || len(baseCfg.SSLCertKey) == 0 {
			return errMissingParams
		}
		return errors.Wrap(security.RunCreateNodeCert(baseCfg.SSLCA, baseCfg.SSLCAKey,
			baseCfg.SSLCert, baseCfg.SSLCertKey, keySize, args),
			"failed to generate node certificate",
		)*/
	return nil
}

// A createClientCert command generates a client certificate and stores it
// in the cert directory under <username>.crt and key under <username>.key.
var createClientCertCmd = &cobra.Command{
	Use:   "create-client --ca-cert=<ca-cert> --ca-key=<ca-key> --cert=<node-cert> --key=<node-key> username",
	Short: "create client cert and key",
	Long: `
Generates a client certificate and key, writing them to --cert and --key.
CA certificate and key must be passed in.
The certs directory should contain a CA cert and key.
`,
	RunE: MaybeDecorateGRPCError(runCreateClientCert),
}

// runCreateClientCert generates key pair and CA certificate and writes them
// to their corresponding files.
func runCreateClientCert(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return usageAndError(cmd)
	}

	var err error
	//var username string
	//if username, err = sql.NormalizeAndValidateUsername(args[0]); err != nil {
	if _, err = sql.NormalizeAndValidateUsername(args[0]); err != nil {
		return err
	}

	// TODO(mberhault): fix
	/*
		if len(baseCfg.SSLCA) == 0 || len(baseCfg.SSLCAKey) == 0 ||
			len(baseCfg.SSLCert) == 0 || len(baseCfg.SSLCertKey) == 0 {
			return errMissingParams
		}

		return errors.Wrap(security.RunCreateClientCert(baseCfg.SSLCA, baseCfg.SSLCAKey,
			baseCfg.SSLCert, baseCfg.SSLCertKey, keySize, username),
			"failed to generate client certificate",
		)*/
	return nil
}

// A listCerts command generates a client certificate and stores it
// in the cert directory under <username>.crt and key under <username>.key.
// TODO(marc): rename once certificate_manager is being used.
var listCertsCmd = &cobra.Command{
	Use:   "list",
	Short: "list certs in --certs-dir",
	Long: `
List certificates and keys found in the certificate directory.
`,
	RunE: MaybeDecorateGRPCError(runListCerts),
}

// runListCerts loads and lists all certs.
func runListCerts(cmd *cobra.Command, args []string) error {
	if len(args) != 0 {
		return usageAndError(cmd)
	}

	cm, err := baseCfg.GetCertificateManager()
	if err != nil {
		return errors.Wrap(err, "could not get certificate manager")
	}

	fmt.Fprintf(os.Stdout, "Certificate directory: %s\n", baseCfg.SSLCertsDir)

	table := tablewriter.NewWriter(os.Stdout)
	table.SetAutoFormatHeaders(false)
	table.SetAutoWrapText(false)
	table.SetHeader([]string{"Usage", "Certificate File", "Key File", "Notes"})

	if ca := cm.CACert(); ca != nil {
		table.Append([]string{ca.FileUsage.String(), ca.Filename, ca.KeyFilename})
	}

	if node := cm.NodeCert(); node != nil {
		table.Append([]string{node.FileUsage.String(), node.Filename, node.KeyFilename})
	}

	for name, cert := range cm.ClientCerts() {
		table.Append([]string{cert.FileUsage.String(), cert.Filename, cert.KeyFilename,
			fmt.Sprintf("user=%s", name)})
	}

	table.Render()

	return nil
}

var certCmds = []*cobra.Command{
	createCACertCmd,
	createNodeCertCmd,
	createClientCertCmd,
	listCertsCmd,
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
