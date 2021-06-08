// Copyright 2021 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/pgurl"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/ttycolor"
	"github.com/spf13/cobra"
)

var convertURLCmd = &cobra.Command{
	Use: "convert-url <url>",
	Example: `
  convert-url --url postgres://root@localhost:26257/defaultdb

  convert-url "postgresql://example.com?sslcert=certs%2Fclient.root.crt&sslkey=certs%2Fclient.root.key&sslmode=verify-full&sslrootcert=certs%2Fca.crt"
`,

	Short: "convert a SQL connection string for use with various client drivers",
	Args:  cobra.NoArgs,
	RunE:  MaybeDecorateGRPCError(runConvertURL),
}

func runConvertURL(cmd *cobra.Command, _ []string) error {
	var u *pgurl.URL
	if convertCtx.url == "" {
		fmt.Println("# WARNING: no URL specified via --url; using a random URL as example.")
		fmt.Println()

		u = pgurl.New()
	} else {
		var err error
		u, err = pgurl.Parse(convertCtx.url)
		if err != nil {
			return err
		}
	}
	u.
		WithDefaultUsername(security.RootUser).
		WithDefaultDatabase(catalogkeys.DefaultDatabaseName).
		WithDefaultHost("localhost").
		WithDefaultPort(cliCtx.clientConnPort)

	if err := u.Validate(); err != nil {
		return err
	}

	cp := ttycolor.StdoutProfile
	yc := cp[ttycolor.Yellow]
	rc := cp[ttycolor.Reset]

	fmt.Printf("# Connection URL for libpq (%[1]sC/C++%[2]s), psycopg (%[1]sPython%[2]s), lib/pq & pgx (%[1]sGo%[2]s), node-postgres (%[1]sJS%[2]s) and most pq-compatible drivers:\n", yc, rc)
	fmt.Println(u.ToPQ())
	fmt.Println()

	fmt.Printf("# Connection %[1]sDSN (Data Source Name)%[2]s for Postgres drivers that accept DSNs - most drivers and also %[1]sODBC%[2]s:\n", yc, rc)
	fmt.Println(u.ToDSN())
	fmt.Println()

	fmt.Printf("# Connection URL for JDBC (%[1]sJava%[2]s and %[1]sJVM%[2]s-based languages):\n", yc, rc)
	fmt.Println(u.ToJDBC())
	fmt.Println()

	return nil
}
