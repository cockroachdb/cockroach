// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// mtCreateTenantCACertCmd generates a tenant CA certificate and stores it
// in the cert directory.
var mtCreateTenantCACertCmd = &cobra.Command{
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
			security.CreateTenantCAPair(
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
var mtCreateTenantCertCmd = &cobra.Command{
	Use:   "create-tenant-client --certs-dir=<path to cockroach certs dir> --ca-key=<path-to-ca-key> <tenant-id> <host 1> <host 2> ... <host N>",
	Short: "create tenant client certificate and key",
	Long: `
Generate a tenant client certificate "<certs-dir>/client-tenant.<tenant-id>.crt" and key
"<certs-dir>/client-tenant.<tenant-id>.key".

If --overwrite is true, any existing files are overwritten.

Requires a CA cert in "<certs-dir>/ca-client-tenant.crt" and matching key in "--ca-key".
If "ca-client-tenant.crt" contains more than one certificate, the first is used.
Creation fails if the CA expiration time is before the desired certificate expiration.

If no server addresses are passed, then a default list containing 127.0.0.1, ::1, localhost and *.local is used.
`,
	Args: cobra.MinimumNArgs(1),
	RunE: clierrorplus.MaybeDecorateError(
		func(cmd *cobra.Command, args []string) error {
			tenantIDs := args[0]

			var hostAddrs []string
			if len(args) > 1 {
				hostAddrs = args[1:]
			} else {
				// Default list.
				// We need this default because of this CI problem:
				// https://github.com/cockroachdb/cockroach/issues/71387
				//
				// If/when this issue is fixed, the command can be updated to
				// not provide a default any more (which would be less error
				// prone.)
				hostAddrs = []string{
					"127.0.0.1",
					"::1",
					"localhost",
					"*.local",
				}
				fmt.Fprintf(stderr, "Warning: no server address specified. Using %+v.\n", hostAddrs)
			}

			tenantID, err := strconv.ParseUint(tenantIDs, 10, 64)
			if err != nil {
				return errors.Wrapf(err, "%s is invalid uint64", tenantIDs)
			}
			cp, err := security.CreateTenantPair(
				certCtx.certsDir,
				certCtx.caKey,
				certCtx.keySize,
				certCtx.certificateLifetime,
				tenantID,
				hostAddrs,
			)
			if err != nil {
				return errors.Wrap(
					err,
					"failed to generate tenant client certificate and key")
			}
			return errors.Wrap(
				security.WriteTenantPair(certCtx.certsDir, cp, certCtx.overwriteFiles),
				"failed to write tenant client certificate and key")
		}),
}

// A mtCreateTenantSigningCertCmd command generates a signing
// certificate and stores it in the cert directory under
// tenant-signing.<ID>.crt and key under tenant-signing.<ID>.key.
var mtCreateTenantSigningCertCmd = &cobra.Command{
	Use:   "create-tenant-signing --certs-dir=<path to cockroach certs dir> <tenant-id>",
	Short: "create tenant signing certificate and key",
	Long: `
Generate a tenant signing certificate "<certs-dir>/tenant-signing.<tenant-id>.crt" and signing key
"<certs-dir>/tenant-signing.<tenant-id>.key".

If --overwrite is true, any existing files are overwritten.
`,
	Args: cobra.ExactArgs(1),
	RunE: clierrorplus.MaybeDecorateError(
		func(cmd *cobra.Command, args []string) error {
			tenantIDs := args[0]
			tenantID, err := strconv.ParseUint(tenantIDs, 10, 64)
			if err != nil {
				return errors.Wrapf(err, "%s is invalid uint64", tenantIDs)
			}
			return errors.Wrap(
				security.CreateTenantSigningPair(
					certCtx.certsDir,
					certCtx.certificateLifetime,
					certCtx.overwriteFiles,
					tenantID),
				"failed to generate tenant signing cert and key")
		}),
}
