// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/username"
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
	RunE:  clierrorplus.MaybeDecorateError(runConvertURL),
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
	if convertCtx.database != "" {
		u.WithDatabase(convertCtx.database)
	}
	if convertCtx.username != "" {
		u.WithUsername(convertCtx.username)
	}
	if convertCtx.password != "" {
		u.WithAuthn(pgurl.AuthnPassword(true, convertCtx.password))
	}
	// If no username/database were specified in the options, and the URL didn't
	// specify them either, we set some sensible defaults.
	u.
		WithDefaultUsername(username.RootUser).
		WithDefaultDatabase(catalogkeys.DefaultDatabaseName).
		WithDefaultHost("localhost").
		WithDefaultPort(cliCtx.clientOpts.ServerPort)

	if err := setURLCertOptions(u); err != nil {
		return err
	}

	if convertCtx.cluster != "" {
		if err := u.SetOption("options", "-ccluster="+convertCtx.cluster); err != nil {
			return err
		}
	}

	if err := u.Validate(); err != nil {
		return err
	}

	cp := ttycolor.StdoutProfile
	yc := cp[ttycolor.Yellow]
	rc := cp[ttycolor.Reset]

	crdbURL, err := u.ToCRDB(convertCtx.sslInline)
	if err != nil {
		return err
	}

	fmt.Printf("# Connection URL for libpq (%[1]sC/C++%[2]s), psycopg (%[1]sPython%[2]s), lib/pq & pgx (%[1]sGo%[2]s), node-postgres (%[1]sJS%[2]s) and most pq-compatible drivers:\n", yc, rc)
	fmt.Println(u.ToPQ())
	fmt.Println()

	fmt.Printf("# Connection %[1]sDSN (Data Source Name)%[2]s for Postgres drivers that accept DSNs - most drivers and also %[1]sODBC%[2]s:\n", yc, rc)
	fmt.Println(u.ToDSN())
	fmt.Println()

	fmt.Printf("# Connection URL for JDBC (%[1]sJava%[2]s and %[1]sJVM%[2]s-based languages):\n", yc, rc)
	fmt.Println(u.ToJDBC())
	fmt.Println()

	fmt.Printf("# Connection URL for %[1]sCRDB%[2]s streams (%[1]sPhysical/Logical Cluster Replication%[2]s):\n", yc, rc)
	fmt.Println(crdbURL)

	return nil
}

// setURLCertOptions modifies the given URL to include any SSL-options that can
// be resolved from the command-line options.
func setURLCertOptions(u *pgurl.URL) error {
	sqlUser, err := username.MakeSQLUsernameFromPreNormalizedStringChecked(u.GetUsername())
	if err != nil {
		return err
	}

	var sslmode string
	caCertPath := convertCtx.caCertPath
	certPath := convertCtx.certPath
	keyPath := convertCtx.keyPath

	if convertCtx.certsDir != "" {
		cm, err := security.NewCertificateManager(convertCtx.certsDir, security.CommandTLSSettings{})
		if err != nil {
			return err
		}
		if caCertPath == "" {
			caCertPath = cm.CACertPath()
		}
		if certPath == "" {
			certPath = cm.ClientCertPath(sqlUser)
		}
		if keyPath == "" {
			keyPath = cm.ClientKeyPath(sqlUser)
		}
	}
	if caCertPath != "" {
		sslmode = "verify-full"
	}

	if caCertPath != "" {
		if err := u.SetOption("sslrootcert", caCertPath); err != nil {
			return err
		}
	}
	if certPath != "" {
		if err := u.SetOption("sslcert", certPath); err != nil {
			return err
		}
	}
	if keyPath != "" {
		if err := u.SetOption("sslkey", keyPath); err != nil {
			return err
		}
	}
	if sslmode != "" {
		if err := u.SetOption("sslmode", sslmode); err != nil {
			return err
		}
	}

	return nil
}
