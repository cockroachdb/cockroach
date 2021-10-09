// Copyright 2020 The Cockroach Authors.
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
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// mtCreateTenantClientCACertCmd generates a tenant CA certificate and stores it
// in the cert directory.
var mtCreateTenantClientCACertCmd = &cobra.Command{
	Use:   "create-tenant-client-ca --certs-dir=<path to cockroach certs dir> --ca-key=<path>",
	Short: "create tenant client CA certificate and key",
	Long: `
Generate a tenant client CA certificate "<certs-dir>/ca-client-tenant.crt" and CA key "<path>".
The certs directory is created if it does not exist.

If the CA key exists and --allow-ca-key-reuse is true, the key is used.
If the CA certificate exists and --overwrite is true, the new CA certificate is prepended to it.
`,
	Args: cobra.NoArgs,
	RunE: clierrorplus.MaybeDecorateError(func(cmd *cobra.Command, args []string) error {
		return errors.Wrap(
			security.CreateTenantClientCAPair(
				certCtx.certsDir,
				certCtx.caKey,
				certCtx.keySize,
				certCtx.caCertificateLifetime,
				certCtx.allowCAKeyReuse,
				certCtx.overwriteFiles),
			"failed to generate tenant client CA cert and key")
	}),
}

// A createClientCert command generates a client certificate and stores it
// in the cert directory under <username>.crt and key under <username>.key.
var mtCreateTenantClientCertCmd = &cobra.Command{
	Use:   "create-tenant-client --certs-dir=<path to cockroach certs dir> --ca-key=<path-to-ca-key> <tenant-id>",
	Short: "create tenant client certificate and key",
	Long: `
Generate a tenant client certificate "<certs-dir>/client-tenant.<tenant-id>.crt" and key
"<certs-dir>/client-tenant.<tenant-id>.key".

If --overwrite is true, any existing files are overwritten.

Requires a CA cert in "<certs-dir>/ca-client-tenant.crt" and matching key in "--ca-key".
If "ca-client-tenant.crt" contains more than one certificate, the first is used.
Creation fails if the CA expiration time is before the desired certificate expiration.
`,
	Args: cobra.ExactArgs(1),
	RunE: clierrorplus.MaybeDecorateError(
		func(cmd *cobra.Command, args []string) error {
			tenantID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return errors.Wrapf(err, "%s is invalid uint64", args[0])
			}
			cp, err := security.CreateTenantClientPair(
				certCtx.certsDir,
				certCtx.caKey,
				certCtx.keySize,
				certCtx.certificateLifetime,
				tenantID,
			)
			if err != nil {
				return errors.Wrap(
					err,
					"failed to generate tenant client certificate and key")
			}
			return errors.Wrap(
				security.WriteTenantClientPair(certCtx.certsDir, cp, certCtx.overwriteFiles),
				"failed to write tenant client certificate and key")
		}),
}

// A mtCreateSQLNodeCert command generates a SQL server node certificate
// and stores it in the cert directory.
var mtCreateSQLNodeCertCmd = &cobra.Command{
	Use:   "create-tenant-node --certs-dir=<path to cockroach certs dir> --ca-key=<path-to-ca-key> <host 1> <host 2> ... <host N>",
	Short: "create SQL server certificate and key",
	Long: `
Generate a node certificate "<certs-dir>/sql-node.crt" and key "<certs-dir>/sql-node.key".

If --overwrite is true, any existing files are overwritten.

At least one host should be passed in (either IP address or dns name).

Requires a CA cert in "<certs-dir>/ca.crt" and matching key in "--ca-key".
If "ca.crt" contains more than one certificate, the first is used.
Creation fails if the CA expiration time is before the desired certificate expiration.
`,
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return errors.Errorf("create-sql-node requires at least one host name or address, none was specified")
		}
		return nil
	},
	RunE: clierrorplus.MaybeDecorateError(runCreateSQLNodeCert),
}

// runCreateSQLNodeCert generates key pair and CA certificate and writes them
// to their corresponding files.
// TODO(marc): there is currently no way to specify which CA cert to use if more
// than one is present. We shoult try to load each certificate along with the key
// and pick the one that works. That way, the key specifies the certificate.
func runCreateSQLNodeCert(cmd *cobra.Command, args []string) error {
	return errors.Wrap(
		security.CreateSQLNodePair(
			certCtx.certsDir,
			certCtx.caKey,
			certCtx.keySize,
			certCtx.certificateLifetime,
			certCtx.overwriteFiles,
			args),
		"failed to generate SQL server certificate and key")
}
