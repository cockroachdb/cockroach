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
	"context"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/redact"
	"github.com/spf13/cobra"
)

var mtStartSQLCmd = &cobra.Command{
	Use:   "start-sql",
	Short: "start a standalone SQL server",
	Long: `
Start a standalone SQL server.

This functionality is **experimental** and for internal use only.

The following certificates are required:

- ca.crt, node.{crt,key}: CA cert and key pair for serving the SQL endpoint.
  Note that under no circumstances should the node certs be shared with those of
  the same name used at the KV layer, as this would pose a severe security risk.
- ca-client-tenant.crt, client-tenant.X.{crt,key}: CA cert and key pair for
  authentication and authorization with the KV layer (as tenant X).
- ca-server-tenant.crt: to authenticate KV layer.

                 ca.crt        ca-client-tenant.crt        ca-server-tenant.crt
user ---------------> sql server ----------------------------> kv
 client.Y.crt    node.crt      client-tenant.X.crt         server-tenant.crt
 client.Y.key    node.key      client-tenant.X.key         server-tenant.key

Note that CA certificates need to be present on the "other" end of the arrow as
well unless it can be verified using a trusted root certificate store. That is,

- ca.crt needs to be passed in the Postgres connection string (sslrootcert) if
  sslmode=verify-ca.
- ca-server-tenant.crt needs to be present on the SQL server.
- ca-client-tenant.crt needs to be present on the KV server.
`,
	Args: cobra.NoArgs,
	RunE: clierrorplus.MaybeDecorateError(runStartSQL),
}

func runStartSQL(cmd *cobra.Command, args []string) error {
	const serverType redact.SafeString = "SQL server"

	initConfig := func(ctx context.Context) error {
		if err := serverCfg.InitSQLServer(ctx); err != nil {
			return err
		}

		// We need a value in the version setting prior to the update
		// coming from the system.settings table. This value must be valid
		// and compatible with the state of the tenant's keyspace.
		//
		// Since we don't know at which binary version the tenant
		// keyspace was initialized, we must be conservative and
		// assume it was created a long time ago; and that we may
		// have to run all known migrations since then. So initialize
		// the version setting to the minimum supported version.
		st := serverCfg.BaseConfig.Settings
		return clusterversion.Initialize(
			ctx, st.Version.BinaryMinSupportedVersion(), &st.SV,
		)
	}

	newServerFn := func(ctx context.Context, serverCfg server.Config, stopper *stop.Stopper) (serverStartupInterface, error) {
		// Beware of not writing simply 'return server.NewServer()'. This is
		// because it would cause the serverStartupInterface reference to
		// always be non-nil, even if NewServer returns a nil pointer (and
		// an error). The code below is dependent on the interface
		// reference remaining nil in case of error.
		s, err := server.NewSeparateProcessTenantServer(
			ctx,
			stopper,
			serverCfg.BaseConfig,
			serverCfg.SQLConfig,
			nil, /* tenantNameContainer */
		)
		if err != nil {
			return nil, err
		}
		return s, nil
	}

	return runStartInternal(cmd, serverType, initConfig, newServerFn, false /* startSingleNode */)
}
