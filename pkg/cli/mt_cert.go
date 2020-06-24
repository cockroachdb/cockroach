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
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// mtCreateTenantServerCACertCmd generates a tenant CA certificate and stores it
// in the cert directory.
var mtCreateTenantServerCACertCmd = &cobra.Command{
	Use:   "create-tenant-server-ca --certs-dir=<path to cockroach certs dir> --ca-key=<path>",
	Short: "create tenant server CA certificate and key",
	Long: `
Generate a tenant server CA certificate "<certs-dir>/ca-server-tenant.crt" and CA key "<path>".
The certs directory is created if it does not exist.

If the CA key exists and --allow-ca-key-reuse is true, the key is used.
If the CA certificate exists and --overwrite is true, the new CA certificate is prepended to it.
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(func(cmd *cobra.Command, args []string) error {
		return errors.Wrap(
			security.CreateTenantServerCAPair(
				baseCfg.SSLCertsDir,
				baseCfg.SSLCAKey,
				keySize,
				caCertificateLifetime,
				allowCAKeyReuse,
				overwriteFiles),
			"failed to generate tenant server CA cert and key")
	}),
}

var mtCreateTenantServerCertCmd = &cobra.Command{
	Use:   "create-tenant-server --certs-dir=<path to cockroach certs dir> --ca-key=<path> <host 1> <host 2> ... <host N>",
	Short: "create tenant server CA certificate and key",
	Long: `
Generate a tenant certificate "<certs-dir>/server-tenant.crt" and key "<certs-dir>/server-tenant.key".

If --overwrite is true, any existing files are overwritten.

At least one host should be passed in (either IP address or dns name).

Requires a CA cert in "<certs-dir>/ca-server-tenant.crt" and matching key in "--ca-key".
If "ca-server-tenant.crt" contains more than one certificate, the first is used.
Creation fails if the CA expiration time is before the desired certificate expiration.
`,
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return errors.Errorf("create-node requires at least one host name or address, none was specified")
		}
		return nil
	},
	RunE: MaybeDecorateGRPCError(func(cmd *cobra.Command, args []string) error {
		return errors.Wrap(
			security.CreateTenantServerPair(
				baseCfg.SSLCertsDir,
				baseCfg.SSLCAKey,
				keySize,
				certificateLifetime,
				overwriteFiles,
				args),
			"failed to generate tenant server certificate and key")
	}),
}

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
	RunE: MaybeDecorateGRPCError(func(cmd *cobra.Command, args []string) error {
		return errors.Wrap(
			security.CreateTenantClientCAPair(
				baseCfg.SSLCertsDir,
				baseCfg.SSLCAKey,
				keySize,
				caCertificateLifetime,
				allowCAKeyReuse,
				overwriteFiles),
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
	RunE: MaybeDecorateGRPCError(
		func(cmd *cobra.Command, args []string) error {
			cp, err := security.CreateTenantClientPair(
				baseCfg.SSLCertsDir,
				baseCfg.SSLCAKey,
				keySize,
				certificateLifetime,
				args[0],
			)
			if err != nil {
				return errors.Wrap(
					err,
					"failed to generate client certificate and key")
			}
			return errors.Wrap(
				security.WriteTenantClientPair(baseCfg.SSLCertsDir, cp, overwriteFiles),
				"failed to write tenant client certificate and key")
		}),
}
