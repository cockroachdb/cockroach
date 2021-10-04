package sql_test

import (
	"context"
	gosql "database/sql"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestNFCNormalization(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	cluster := serverutils.StartNewTestCluster(
		t,
		1,
		base.TestClusterArgs{},
	)
	defer cluster.Stopper().Stop(context.Background())

	pgUrl, cleanupFn := sqlutils.PGUrl(
		t,
		cluster.Server(0).ServingSQLAddr(),
		"NFC Normalization",
		url.User("root"),
	)
	defer cleanupFn()

	db, err := gosql.Open("postgres", pgUrl.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE \"" + string("caf\u00E9") + "\" (a INT)")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE \"" + string("cafe\u0301") + "\" (a INT)")
	require.Errorf(t, err, "The tables should be considered duplicates when normalized")
}
