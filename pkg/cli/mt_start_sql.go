// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverctl"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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
		return serverCfg.InitSQLServer(ctx)
	}

	newServerFn := func(ctx context.Context, serverCfg server.Config, stopper *stop.Stopper) (serverctl.ServerStartupInterface, error) {
		// The context passed into NewSeparateProcessTenantServer will be
		// captured in makeTenantSQLServerArgs by a few users that will hold on
		// to it. Those users can log something into the current tracing span
		// (created in runStartInternal) after the span has been Finish()'ed
		// which is disallowed. To go around this issue, in this code path we
		// create a new tracing span that is never finished since those users
		// stay alive until the tenant server is up.
		// TODO(yuzefovich): consider changing NewSeparateProcessTenantServer to
		// not take in a context and - akin to NewServer - to construct its own
		// from context.Background.
		ctx, _ = tracing.ChildSpan(ctx, "mt-start-sql" /* opName */)
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
