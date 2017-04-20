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
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

const defaultKeySize = 2048

// We use 366 days on certificate lifetimes to at least match X years,
// otherwise leap years risk putting us just under.
const defaultCALifetime = 5 * 366 * 24 * time.Hour   // five years
const defaultCertLifetime = 2 * 366 * 24 * time.Hour // two years

var keySize int
var certificateLifetime time.Duration
var allowCAKeyReuse bool
var overwriteFiles bool

// A createCACert command generates a CA certificate and stores it
// in the cert directory.
var createCACertCmd = &cobra.Command{
	Use:   "create-ca --certs-dir=<path to cockroach certs dir> --ca-key=<path-to-ca-key>",
	Short: "create CA certificate and key",
	Long: `
Generate a CA certificate "<certs-dir>/ca.crt" and CA key "<ca-key>".
The certs directory is created if it does not exist.

If the CA key exists and --allow-ca-key-reuse is true, the key is used.
If the CA certificate exists and --overwrite is true, the new CA certificate is prepended to it.
`,
	RunE: MaybeDecorateGRPCError(runCreateCACert),
}

// runCreateCACert generates a key and CA certificate and writes them
// to their corresponding files.
func runCreateCACert(cmd *cobra.Command, args []string) error {
	return errors.Wrap(
		security.CreateCAPair(
			baseCfg.SSLCertsDir,
			baseCfg.SSLCAKey,
			keySize,
			certificateLifetime,
			allowCAKeyReuse,
			overwriteFiles),
		"failed to generate CA cert and key")
}

// A createNodeCert command generates a node certificate and stores it
// in the cert directory.
var createNodeCertCmd = &cobra.Command{
	Use:   "create-node --certs-dir=<path to cockroach certs dir> --ca-key=<path-to-ca-key> <host 1> <host 2> ... <host N>",
	Short: "create node certificate and key",
	Long: `
Generate a node certificate "<certs-dir>/node.crt" and key "<certs-dir>/node.key".

If --overwrite is true, any existing files are overwritten.

At least one host should be passed in (either IP address or dns name).

Requires a CA cert in "<certs-dir>/ca.crt" and matching key in "--ca-key".
If "ca.crt" contains more than one certificate, the first is used.
`,
	RunE: MaybeDecorateGRPCError(runCreateNodeCert),
}

// runCreateNodeCert generates key pair and CA certificate and writes them
// to their corresponding files.
// TODO(marc): there is currently no way to specify which CA cert to use if more
// than one is present. We shoult try to load each certificate along with the key
// and pick the one that works. That way, the key specifies the certificate.
func runCreateNodeCert(cmd *cobra.Command, args []string) error {
	return errors.Wrap(
		security.CreateNodePair(
			baseCfg.SSLCertsDir,
			baseCfg.SSLCAKey,
			keySize,
			certificateLifetime,
			overwriteFiles,
			args),
		"failed to generate node certificate and key")
}

// A createClientCert command generates a client certificate and stores it
// in the cert directory under <username>.crt and key under <username>.key.
var createClientCertCmd = &cobra.Command{
	Use:   "create-client --certs-dir=<path to cockroach certs dir> --ca-key=<path-to-ca-key> <username>",
	Short: "create client certificate and key",
	Long: `
Generate a client certificate "<certs-dir>/client.<username>.crt" and key
"<certs-dir>/client.<username>.key".

If --overwrite is true, any existing files are overwritten.

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
		return errors.Wrap(err, "failed to generate client certificate and key")
	}

	return errors.Wrap(
		security.CreateClientPair(
			baseCfg.SSLCertsDir,
			baseCfg.SSLCAKey,
			keySize,
			certificateLifetime,
			overwriteFiles,
			username),
		"failed to generate client certificate and key")
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

	certTableHeaders := []string{"Usage", "Certificate File", "Key File", "Notes"}
	var rows [][]string

	if ca := cm.CACert(); ca != nil {
		rows = append(rows, []string{ca.FileUsage.String(), ca.Filename, ca.KeyFilename, ""})
	}

	if node := cm.NodeCert(); node != nil {
		rows = append(rows, []string{node.FileUsage.String(), node.Filename, node.KeyFilename, ""})
	}

	for name, cert := range cm.ClientCerts() {
		rows = append(rows, []string{cert.FileUsage.String(), cert.Filename, cert.KeyFilename,
			fmt.Sprintf("user=%s", name)})
	}

	printQueryOutput(os.Stdout, certTableHeaders, rows, "", cliCtx.tableDisplayFormat)
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
