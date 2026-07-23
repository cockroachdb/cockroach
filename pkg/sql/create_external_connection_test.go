// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestCreateExternalConnectionGrantNameEscaping verifies that a connection name
// containing a double quote cannot break out of the identifier in the GRANT
// statement that createExternalConnection runs as the node user, which would
// otherwise let the creator grant themselves privileges on a connection they do
// not own.
func TestCreateExternalConnectionGrantNameEscaping(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	srv := s.ApplicationLayer()

	adminDB := sqlutils.MakeSQLRunner(db)
	// A connection owned by another user; the attacker must not gain any
	// privilege on it.
	adminDB.Exec(t, `CREATE EXTERNAL CONNECTION victim AS 'userfile:///'`)

	const password = "correcthorsebatterystaple"
	adminDB.Exec(t, "CREATE USER attacker WITH PASSWORD $1", password)
	adminDB.Exec(t, "GRANT SYSTEM EXTERNALCONNECTION TO attacker")

	attackerDB := sqlutils.MakeSQLRunner(srv.SQLConn(
		t, serverutils.UserPassword("attacker", password), serverutils.ClientCerts(false),
	))
	// A connection the attacker legitimately owns, used as the first entry of
	// the crafted object list so the injected GRANT resolves.
	attackerDB.Exec(t, `CREATE EXTERNAL CONNECTION evil AS 'userfile:///'`)
	// The crafted name closes the identifier early. Without escaping, the
	// node-run GRANT becomes GRANT ALL ON EXTERNAL CONNECTION "evil" , "victim"
	// TO attacker, handing the attacker ALL on the victim connection.
	attackerDB.Exec(t, `CREATE EXTERNAL CONNECTION "evil"" , ""victim" AS 'userfile:///'`)

	// The attacker should only hold privileges on the connections it created
	// (each stored under its literal name), never on victim.
	adminDB.CheckQueryResults(t,
		`SELECT path FROM system.privileges WHERE username = 'attacker' ORDER BY path`,
		[][]string{{`/externalconn/evil`}, {`/externalconn/evil" , "victim`}},
	)
}
