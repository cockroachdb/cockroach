// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/security/externalcreds"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// pgErrCode returns the pgcode of err as reported by the SQL driver, or the
// empty string if err is not a *pq.Error.
func pgErrCode(err error) string {
	var pqErr *pq.Error
	if !errors.As(err, &pqErr) {
		return ""
	}
	return string(pqErr.Code)
}

// fileBasedClientAssertionSinkURI builds a PROPRIETARY_OAUTH file-mode sink
// URI. jwtRelPath is interpreted relative to the node's
// --external-credentials-dir.
func fileBasedClientAssertionSinkURI(t *testing.T, jwtRelPath string) string {
	t.Helper()
	return fmt.Sprintf(
		"kafka://127.0.0.1:1/?tls_enabled=true&sasl_enabled=true"+
			"&sasl_mechanism=PROPRIETARY_OAUTH"+
			"&sasl_client_id=c"+
			"&sasl_token_url=https://idp/token"+
			"&sasl_proprietary_resource=r"+
			"&sasl_proprietary_client_assertion_type=at"+
			"&sasl_proprietary_client_assertion_location=%s",
		url.QueryEscape(jwtRelPath),
	)
}

func TestChangefeedFileBasedCredentialPrivilege(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	secretDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(secretDir, "jwt"), []byte("dummy"), 0o600))

	ecDir, err := externalcreds.NewDir(secretDir)
	require.NoError(t, err)
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		ExternalCredentialsDir: ecDir,
	})
	defer srv.Stopper().Stop(ctx)

	sqlutils.MakeSQLRunner(srv.SystemLayer().SQLConn(t)).Exec(
		t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
	app := srv.ApplicationLayer()
	rootDB := sqlutils.MakeSQLRunner(app.SQLConn(t))
	rootDB.Exec(t, `CREATE TABLE t (x INT PRIMARY KEY)`)
	rootDB.Exec(t, `CREATE USER testuser`)
	rootDB.Exec(t, `GRANT CHANGEFEED ON TABLE t TO testuser`)

	testuser := app.SQLConn(t, serverutils.User("testuser"))
	sinkURI := fileBasedClientAssertionSinkURI(t, "jwt")

	rootDB.Exec(t, fmt.Sprintf(`CREATE EXTERNAL CONNECTION ec AS '%s'`, sinkURI))
	rootDB.Exec(t, `GRANT USAGE ON EXTERNAL CONNECTION ec TO testuser`)
	const ecURI = `external://ec`

	_, err = testuser.ExecContext(ctx, `CREATE CHANGEFEED FOR t INTO $1`, sinkURI)
	require.Equal(t, pgcode.InsufficientPrivilege.String(), pgErrCode(err),
		"expected privilege error on direct URI, got: %v", err)
	require.ErrorContains(t, err, changefeedbase.SinkParamSASLProprietaryClientAssertionLocation)

	_, err = testuser.ExecContext(ctx, `CREATE CHANGEFEED FOR t INTO $1`, ecURI)
	require.Equal(t, pgcode.InsufficientPrivilege.String(), pgErrCode(err),
		"expected privilege error on external connection, got: %v", err)

	// After the grant, the sink dials 127.0.0.1:1 so an error is still
	// expected; what matters is that the privilege gate is no longer the
	// failure reason on either path.
	rootDB.Exec(t, `GRANT SYSTEM EXTERNALIOIMPLICITACCESS TO testuser`)
	for _, uri := range []string{sinkURI, ecURI} {
		_, err = testuser.ExecContext(ctx, `CREATE CHANGEFEED FOR t INTO $1`, uri)
		require.Error(t, err)
		require.NotEqual(t, pgcode.InsufficientPrivilege.String(), pgErrCode(err),
			"unexpected privilege error after grant for %q: %v", uri, err)
	}
}
