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
	"time"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/olekukonko/tablewriter"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

const defaultKeySize = 2048
const defaultCALifetime = 5 * 265 * 24 * time.Hour // five years
const defaultCertLifetime = 265 * 24 * time.Hour   // one year

var keySize int
var certificateLifetime time.Duration

// A createCACert command generates a CA certificate and stores it
// in the cert directory.
var createCACertCmd = &cobra.Command{
	Use:   "create-ca --certs-dir=<path to cockroach certs dir> --ca-key=<path-to-ca-key>",
	Short: "create CA certificate and key",
	Long: `
Generate a CA certificate "<certs-dir>/ca.crt" and CA key "<ca-key>".
The certs directory is created if it does not exist.
The CA key is created if it does not exist.
The CA certificate is prepended to "ca.crt" if the file exists.
`,
	RunE: MaybeDecorateGRPCError(runCreateCACert),
}

// runCreateCACert generates a key and CA certificate and writes them
// to their corresponding files.
func runCreateCACert(cmd *cobra.Command, args []string) error {
	return security.CreateCAPair(
		baseCfg.SSLCertsDir,
		baseCfg.SSLCAKey,
		keySize,
		certificateLifetime)
}

// A createNodeCert command generates a node certificate and stores it
// in the cert directory.
var createNodeCertCmd = &cobra.Command{
	Use:   "create-node --certs-dir=<path to cockroach certs dir> --ca-key=<path-to-ca-key> <host 1> <host 2> ... <host N>",
	Short: "create node certificate and key",
	Long: `
Generate a node certificate "<certs-dir>/node.crt" and key "<certs-dir>/node.key".
Any existing files are overwritten.
At least one host should be passed in (either IP address or dns name).

Requires a CA cert in "<certs-dir>/ca.crt" and matching key in "--ca-key".
If "ca.crt" contains more than one certificate, the first is used.
`,
	RunE: MaybeDecorateGRPCError(runCreateNodeCert),
}

// runCreateNodeCert generates key pair and CA certificate and writes them
// to their corresponding files.
// TODO(marc): there is currently no way to specify which CA cert to use if more
// than one if present.
func runCreateNodeCert(cmd *cobra.Command, args []string) error {
	return security.CreateNodePair(
		baseCfg.SSLCertsDir,
		baseCfg.SSLCAKey,
		keySize,
		certificateLifetime,
		args)
}

// A createClientCert command generates a client certificate and stores it
// in the cert directory under <username>.crt and key under <username>.key.
var createClientCertCmd = &cobra.Command{
	Use:   "create-client --certs-dir=<path to cockroach certs dir> --ca-key=<path-to-ca-key> <username>",
	Short: "create client certificate and key",
	Long: `
Generate a client certificate "<certs-dir>/client.<username>.crt" and key
"<certs-dir>/client.<username>.key".
Any existing files are overwritten.

Requires a CA cert in "<certs-dir>/ca.crt" and matching key in "--ca-key".
If "ca.crt" contains more than one certificate, the first is used.
`,
	RunE: MaybeDecorateGRPCError(runCreateClientCert),
}

// runCreateClientCert generates key pair and CA certificate and writes them
// to their corresponding files.
// TODO(marc): there is currently no way to specify which CA cert to use if more
// than one if present.
func runCreateClientCert(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return usageAndError(cmd)
	}

	var err error
	var username string
	if username, err = sql.NormalizeAndValidateUsername(args[0]); err != nil {
		return err
	}

	return security.CreateClientPair(
		baseCfg.SSLCertsDir,
		baseCfg.SSLCAKey,
		keySize,
		certificateLifetime,
		username)
}

// A listCerts command generates a client certificate and stores it
// in the cert directory under <username>.crt and key under <username>.key.
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
