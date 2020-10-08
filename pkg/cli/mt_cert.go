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
			tenantID, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return errors.Wrapf(err, "%s is invalid uint64", args[0])
			}
			cp, err := security.CreateTenantClientPair(
				baseCfg.SSLCertsDir,
				baseCfg.SSLCAKey,
				keySize,
				certificateLifetime,
				tenantID,
			)
			if err != nil {
				return errors.Wrap(
					err,
					"failed to generate tenant client certificate and key")
			}
			return errors.Wrap(
				security.WriteTenantClientPair(baseCfg.SSLCertsDir, cp, overwriteFiles),
				"failed to write tenant client certificate and key")
		}),
}
