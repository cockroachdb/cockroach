// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

const defaultKeySize = 2048

// We use 366 days on certificate lifetimes to at least match X years,
// otherwise leap years risk putting us just under.
const defaultCALifetime = 10 * 366 * 24 * time.Hour  // ten years
const defaultCertLifetime = 5 * 366 * 24 * time.Hour // five years

// Options settable via command-line flags. See below for defaults.
var keySize int
var caCertificateLifetime time.Duration
var certificateLifetime time.Duration
var allowCAKeyReuse bool
var overwriteFiles bool
var generatePKCS8Key bool

func initPreFlagsCertDefaults() {
	keySize = defaultKeySize
	caCertificateLifetime = defaultCALifetime
	certificateLifetime = defaultCertLifetime
	allowCAKeyReuse = false
	overwriteFiles = false
	generatePKCS8Key = false
}

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
	Args: cobra.NoArgs,
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
			caCertificateLifetime,
			allowCAKeyReuse,
			overwriteFiles),
		"failed to generate CA cert and key")
}

// A createClientCACert command generates a client CA certificate and stores it
// in the cert directory.
var createClientCACertCmd = &cobra.Command{
	Use:   "create-client-ca --certs-dir=<path to cockroach certs dir> --ca-key=<path-to-client-ca-key>",
	Short: "create client CA certificate and key",
	Long: `
Generate a client CA certificate "<certs-dir>/ca-client.crt" and CA key "<client-ca-key>".
The certs directory is created if it does not exist.

If the CA key exists and --allow-ca-key-reuse is true, the key is used.
If the CA certificate exists and --overwrite is true, the new CA certificate is prepended to it.

The client CA is optional and should only be used when separate CAs are desired for server certificates
and client certificates.

If the client CA exists, a client.node.crt client certificate must be created using:
  cockroach cert create-client node

Once the client.node.crt exists, all client certificates will be verified using the client CA.
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(runCreateClientCACert),
}

// runCreateClientCACert generates a key and CA certificate and writes them
// to their corresponding files.
func runCreateClientCACert(cmd *cobra.Command, args []string) error {
	return errors.Wrap(
		security.CreateClientCAPair(
			baseCfg.SSLCertsDir,
			baseCfg.SSLCAKey,
			keySize,
			caCertificateLifetime,
			allowCAKeyReuse,
			overwriteFiles),
		"failed to generate client CA cert and key")
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
Creation fails if the CA expiration time is before the desired certificate expiration.
`,
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return errors.Errorf("create-node requires at least one host name or address, none was specified")
		}
		return nil
	},
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
Creation fails if the CA expiration time is before the desired certificate expiration.
`,
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runCreateClientCert),
}

// runCreateClientCert generates key pair and CA certificate and writes them
// to their corresponding files.
// TODO(marc): there is currently no way to specify which CA cert to use if more
// than one if present.
func runCreateClientCert(cmd *cobra.Command, args []string) error {
	username, err := security.MakeSQLUsernameFromUserInput(args[0], security.UsernameCreation)
	if err != nil {
		return errors.Wrap(err, "failed to generate client certificate and key")
	}

	return errors.Wrap(
		security.CreateClientPair(
			baseCfg.SSLCertsDir,
			baseCfg.SSLCAKey,
			keySize,
			certificateLifetime,
			overwriteFiles,
			username,
			generatePKCS8Key),
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
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(runListCerts),
}

// runListCerts loads and lists all certs.
func runListCerts(cmd *cobra.Command, args []string) error {
	cm, err := security.NewCertificateManager(baseCfg.SSLCertsDir, security.CommandTLSSettings{})
	if err != nil {
		return errors.Wrap(err, "cannot load certificates")
	}

	fmt.Fprintf(os.Stdout, "Certificate directory: %s\n", baseCfg.SSLCertsDir)

	certTableHeaders := []string{"Usage", "Certificate File", "Key File", "Expires", "Notes", "Error"}
	alignment := "llllll"
	var rows [][]string

	addRow := func(ci *security.CertInfo, notes string) {
		var errString string
		if ci.Error != nil {
			errString = ci.Error.Error()
		}
		rows = append(rows, []string{
			ci.FileUsage.String(),
			ci.Filename,
			ci.KeyFilename,
			ci.ExpirationTime.Format("2006/01/02"),
			notes,
			errString,
		})
	}

	if cert := cm.CACert(); cert != nil {
		var notes string
		if cert.Error == nil && len(cert.ParsedCertificates) > 0 {
			notes = fmt.Sprintf("num certs: %d", len(cert.ParsedCertificates))
		}
		addRow(cert, notes)
	}

	if cert := cm.ClientCACert(); cert != nil {
		var notes string
		if cert.Error == nil && len(cert.ParsedCertificates) > 0 {
			notes = fmt.Sprintf("num certs: %d", len(cert.ParsedCertificates))
		}
		addRow(cert, notes)
	}

	if cert := cm.UICACert(); cert != nil {
		var notes string
		if cert.Error == nil && len(cert.ParsedCertificates) > 0 {
			notes = fmt.Sprintf("num certs: %d", len(cert.ParsedCertificates))
		}
		addRow(cert, notes)
	}

	if cert := cm.NodeCert(); cert != nil {
		var addresses []string
		if cert.Error == nil && len(cert.ParsedCertificates) > 0 {
			addresses = cert.ParsedCertificates[0].DNSNames
			for _, ip := range cert.ParsedCertificates[0].IPAddresses {
				addresses = append(addresses, ip.String())
			}
		} else {
			addresses = append(addresses, "<unknown>")
		}

		addRow(cert, fmt.Sprintf("addresses: %s", strings.Join(addresses, ",")))
	}

	if cert := cm.UICert(); cert != nil {
		var addresses []string
		if cert.Error == nil && len(cert.ParsedCertificates) > 0 {
			addresses = cert.ParsedCertificates[0].DNSNames
			for _, ip := range cert.ParsedCertificates[0].IPAddresses {
				addresses = append(addresses, ip.String())
			}
		} else {
			addresses = append(addresses, "<unknown>")
		}

		addRow(cert, fmt.Sprintf("addresses: %s", strings.Join(addresses, ",")))
	}

	for _, cert := range cm.ClientCerts() {
		var user string
		if cert.Error == nil && len(cert.ParsedCertificates) > 0 {
			user = cert.ParsedCertificates[0].Subject.CommonName
		} else {
			user = "<unknown>"
		}

		addRow(cert, fmt.Sprintf("user: %s", user))
	}

	return PrintQueryOutput(os.Stdout, certTableHeaders, NewRowSliceIter(rows, alignment))
}

var certCmds = []*cobra.Command{
	createCACertCmd,
	createClientCACertCmd,
	createNodeCertCmd,
	createClientCertCmd,
	listCertsCmd,
}

var certCmd = &cobra.Command{
	Use:   "cert",
	Short: "create ca, node, and client certs",
	RunE:  usageAndErr,
}

func init() {
	certCmd.AddCommand(certCmds...)
}
