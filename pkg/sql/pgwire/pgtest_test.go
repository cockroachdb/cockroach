// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgwire_test

import (
	"context"
	"flag"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl" // register cloud storage providers
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgtest"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

var (
	flagAddr = flag.String("addr", "", "pass a custom postgres address to TestWalk instead of starting an in-memory node")
	flagUser = flag.String("user", "postgres", "username used if -addr is specified")
)

func TestPGTest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Enable enterprise features so READ COMMITTED can be tested.
	defer ccl.TestingEnableEnterprise()()

	if *flagAddr == "" {
		newServer := func() (addr, user string, cleanup func()) {
			ctx := context.Background()
			s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
				DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSharedProcessModeButDoesntYet(
					base.TestTenantProbabilistic, 112960,
				),
				Insecure: true,
			})
			cleanup = func() {
				s.Stopper().Stop(ctx)
			}
			addr = s.ApplicationLayer().AdvSQLAddr()
			user = username.RootUser
			// None of the tests read that much data, so we hardcode the max message
			// size to something small. This lets us test the handling of large
			// query inputs. See the large_input test.
			_, _ = db.ExecContext(ctx, "SET CLUSTER SETTING sql.conn.max_read_buffer_message_size = '32 KiB'")
			return addr, user, cleanup
		}
		pgtest.WalkWithNewServer(t, datapathutils.TestDataPath(t, "pgtest"), newServer)
	} else {
		pgtest.WalkWithRunningServer(t, datapathutils.TestDataPath(t, "pgtest"), *flagAddr, *flagUser)
	}
}
