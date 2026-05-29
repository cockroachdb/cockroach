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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestChangefeedFileBasedCredentialPrivilege(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	secretDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(secretDir, "jwt"), []byte("dummy"), 0o600))

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		SecretDirectory: secretDir,
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
	jwtPath := filepath.Join(secretDir, "jwt")
	sinkURI := fmt.Sprintf(
		"kafka://127.0.0.1:1/?tls_enabled=true&sasl_enabled=true"+
			"&sasl_mechanism=PROPRIETARY_OAUTH"+
			"&sasl_client_id=c"+
			"&sasl_token_url=https://idp/token"+
			"&sasl_proprietary_resource=r"+
			"&sasl_proprietary_client_assertion_type=at"+
			"&sasl_proprietary_client_assertion_location=%s",
		url.QueryEscape(jwtPath),
	)

	_, err := testuser.ExecContext(ctx, `CREATE CHANGEFEED FOR t INTO $1`, sinkURI)
	require.Equal(t, pgcode.InsufficientPrivilege, pgerror.GetPGCode(err),
		"expected privilege error, got: %v", err)
	require.ErrorContains(t, err, changefeedbase.SinkParamSASLProprietaryClientAssertionLocation)

	rootDB.Exec(t, `GRANT SYSTEM EXTERNALIOIMPLICITACCESS TO testuser`)
	_, err = testuser.ExecContext(ctx, `CREATE CHANGEFEED FOR t INTO $1`, sinkURI)
	// The sink dials 127.0.0.1:1 so an error is expected; what matters is
	// that the privilege gate is no longer the failure reason.
	require.Error(t, err)
	require.NotEqual(t, pgcode.InsufficientPrivilege, pgerror.GetPGCode(err),
		"unexpected privilege error after grant: %v", err)
}
