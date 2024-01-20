package sql

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"testing"
)

func TestCreateRoleInfinity(t *testing.T) {
	// Test for create role infinity. #116714
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	_, err := sqlDB.Exec(`
create role chris VALID UNTIL 'infinity';                                                                                                                 
show roles;
select * from pg_catalog.pg_roles;
`)

	if err != nil {
		t.Fatal(err)
	}

}
