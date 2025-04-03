// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgrepl

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

//go:generate ../../util/leaktest/add-leaktest.sh *_test.go

func TestMain(m *testing.M) {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	serverutils.InitTestServerFactory(server.TestServerFactory)
	os.Exit(m.Run())
}

func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	pgURL, cleanup := pgurlutils.PGUrl(t, s.AdvSQLAddr(), "pgrepl_datadriven_test", url.User(username.RootUser))
	defer cleanup()

	cfg, err := pgx.ParseConfig(pgURL.String())
	require.NoError(t, err)
	cfg.RuntimeParams["replication"] = "database"
	ctx := context.Background()

	conn, err := pgx.ConnectConfig(ctx, cfg)
	require.NoError(t, err)

	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			var expectError bool
			args := d.CmdArgs[:0]
			for _, arg := range d.CmdArgs {
				switch arg.Key {
				case "error":
					expectError = true
				default:
					args = append(args, arg)
				}
			}
			d.CmdArgs = args

			switch d.Cmd {
			case "simple_query":
				rows, err := conn.Query(ctx, d.Input, pgx.QueryExecModeSimpleProtocol)
				if expectError {
					require.Error(t, err)
					return err.Error()
				}
				out, err := sqlutils.PGXRowsToDataDrivenOutput(rows)
				rows.Close()
				require.NoError(t, err)
				return out
			case "identify_system":
				// IDENTIFY_SYSTEM needs some redaction to be deterministic.
				rows, err := conn.Query(ctx, "IDENTIFY_SYSTEM", pgx.QueryExecModeSimpleProtocol)
				require.NoError(t, err)
				var sb strings.Builder
				for rows.Next() {
					vals, err := rows.Values()
					require.NoError(t, err)
					for i, val := range vals {
						if i > 0 {
							sb.WriteRune('\n')
						}
						switch rows.FieldDescriptions()[i].Name {
						case "systemid":
							val = "some_cluster_id"
						case "xlogpos":
							val = "some_lsn"
						}
						sb.WriteString(rows.FieldDescriptions()[i].Name)
						sb.WriteString(": ")
						sb.WriteString(fmt.Sprintf("%v", val))
					}
				}
				require.NoError(t, rows.Err())
				rows.Close()
				return sb.String()
			default:
				t.Errorf("unhandled command %s", d.Cmd)
			}
			return ""
		})
	})
}
