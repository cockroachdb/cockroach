// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package democluster

import (
	"bufio"
	"context"
	gosql "database/sql"
	"encoding/json"
	"io"
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/security/certnames"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/pgurl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/redact"
)

// saveWebSessions persists any currently active web session to disk,
// so they can be restored when the demo shell starts again.
func (c *transientCluster) saveWebSessions(ctx context.Context) error {
	return c.doPersistence(ctx, "saving", c.saveWebSessionsInternal)
}

// restoreWebSessions restores any currently active web session from disk.
func (c *transientCluster) restoreWebSessions(ctx context.Context) error {
	return c.doPersistence(ctx, "restoring", c.restoreWebSessionsInternal)
}

func (c *transientCluster) doPersistence(
	ctx context.Context,
	word redact.SafeString,
	fn func(ctx context.Context, filename string, db *gosql.DB) error,
) error {
	if c.demoDir == "" {
		// No directory to save to. Bail.
		return nil
	}

	// Lock the output dir to prevent concurrent demo sessions
	// from stamping each other over.
	cleanup, err := c.lockDir(ctx, c.demoDir, "sessions")
	if err != nil {
		return err
	}
	defer cleanup()

	// We compose the connection URL anew because
	// getNetworkURLForServer() uses the 'demo' account and we want a
	// root connection, using client certs.
	//
	// This will bypass any blockage caused by a mistaken HBA
	// configuration by the user.
	caCert := filepath.Join(c.demoDir, certnames.CACertFilename())
	rootCert := filepath.Join(c.demoDir, certnames.ClientCertFilename(username.RootUserName()))
	rootKey := filepath.Join(c.demoDir, certnames.ClientKeyFilename(username.RootUserName()))
	u := pgurl.New().
		WithDatabase(catconstants.SystemDatabaseName).
		WithUsername(username.RootUser).
		WithAuthn(pgurl.AuthnClientCert(rootCert, rootKey)).
		WithTransport(pgurl.TransportTLS(pgurl.TLSRequire, caCert))

	apply := func(filename string, u *pgurl.URL) error {
		db, err := gosql.Open("postgres", u.ToPQ().String())
		if err != nil {
			return err
		}
		defer db.Close()
		return fn(ctx, filename, db)
	}

	if c.demoCtx.Multitenant && len(c.tenantServers) > 0 && c.tenantServers[0] != nil {
		sqlAddr := c.tenantServers[0].SQLAddr()
		host, port, _ := addr.SplitHostPort(sqlAddr, "")
		u.WithNet(pgurl.NetTCP(host, port))
		// When the server controller is not used, the tenant selection is
		// done via the TCP port number and the options parameter is
		// ignored. If the server controller is used, the options will
		// select the tenant.
		_ = u.SetOption("options", "-ccluster="+demoTenantName)
		if err := apply("sessions.app.txt", u); err != nil {
			return errors.Wrapf(err, "%s for application tenant", word)
		}
	}

	if len(c.servers) > 0 && c.servers[0].TestServerInterface != nil {
		sqlAddr := c.servers[0].AdvSQLAddr()
		host, port, _ := addr.SplitHostPort(sqlAddr, "")
		u.WithNet(pgurl.NetTCP(host, port))
		// See the comment above about options.
		_ = u.SetOption("options", "-ccluster="+catconstants.SystemTenantName)
		return errors.Wrapf(
			apply("sessions.system.txt", u),
			"%s for for system tenant", word)
	}
	return nil
}

type webSessionRow struct {
	ID           int64
	HashedSecret []byte
	Username     string
	ExpiresAt    string
}

// saveWebSessionsInternal saves the sessions for just one tenant to
// the provided filename.
func (c *transientCluster) saveWebSessionsInternal(
	ctx context.Context, filename string, db *gosql.DB,
) error {
	c.infoLog(ctx, "saving sessions")
	rows, err := db.QueryContext(ctx, `
SELECT id, "hashedSecret", username, "expiresAt"
FROM system.web_sessions
WHERE "expiresAt" > now()
AND "revokedAt" IS NULL`)
	if err != nil {
		return err
	}
	defer rows.Close()

	outputFile, err := os.Create(filepath.Join(c.demoDir, filename))
	if err != nil {
		return err
	}
	defer func() {
		if err := outputFile.Close(); err != nil {
			c.warnLog(ctx, "%v", err)
		}
	}()
	buf := bufio.NewWriter(outputFile)
	defer func() {
		if err := buf.Flush(); err != nil {
			c.warnLog(ctx, "%v", err)
		}
	}()

	numSessions := 0
	for rows.Next() {
		var row webSessionRow
		if err := rows.Scan(&row.ID, &row.HashedSecret, &row.Username, &row.ExpiresAt); err != nil {
			return err
		}
		j, err := json.Marshal(row)
		if err != nil {
			return err
		}
		j = append(j, '\n')
		if _, err := buf.Write(j); err != nil {
			return err
		}
		numSessions++
	}

	c.infoLog(ctx, "saved %d sessions to %q", numSessions, filename)

	return nil
}

// restoreWebSessionsInternal restores the sessions for just one
// tenant from the provided filename.
func (c *transientCluster) restoreWebSessionsInternal(
	ctx context.Context, filename string, db *gosql.DB,
) error {
	c.infoLog(ctx, "restoring sessions")

	inputFile, err := os.Open(filepath.Join(c.demoDir, filename))
	if err != nil {
		if oserror.IsNotExist(err) {
			// No file yet. That's OK.
			return nil
		}
		return err
	}
	defer func() {
		if err := inputFile.Close(); err != nil {
			c.warnLog(ctx, "%v", err)
		}
	}()

	buf := bufio.NewReader(inputFile)
	numSessions := 0
	for {
		j, err := buf.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Done reading. Nothing more to do.
				break
			}
			return err
		}

		var row webSessionRow
		if err := json.Unmarshal(j, &row); err != nil {
			return err
		}

		if _, err := db.ExecContext(ctx, `
INSERT INTO system.web_sessions(id, "hashedSecret", username, "expiresAt", user_id)
VALUES ($1, $2, $3, $4, (SELECT user_id FROM system.users WHERE username = $3))`,
			row.ID,
			row.HashedSecret,
			row.Username,
			row.ExpiresAt,
		); err != nil {
			return err
		}
		numSessions++
	}

	c.infoLog(ctx, "restored %d sessions from %q", numSessions, filename)

	return nil
}
