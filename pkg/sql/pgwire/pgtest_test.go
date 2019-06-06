// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package pgwire

import (
	"context"
	"flag"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgtest"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

var (
	flagAddr = flag.String("addr", "", "pass a custom postgres address to TestWalk instead of starting an in-memory node")
	flagUser = flag.String("user", "postgres", "username used if -addr is specified")
)

func TestPGTest(t *testing.T) {
	defer leaktest.AfterTest(t)()

	addr := *flagAddr
	user := *flagUser
	if addr == "" {
		ctx := context.Background()
		s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
			Insecure: true,
		})
		defer s.Stopper().Stop(ctx)

		addr = s.Addr()
		user = security.RootUser
	}

	pgtest.Walk(t, "testdata/pgtest", addr, user)
}
