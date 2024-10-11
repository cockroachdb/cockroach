// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streamproducer

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func TestReplicationManagerRequiresReplicationPrivilege(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	tDB := sqlutils.MakeSQLRunner(sqlDB)

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	testTenants := s.TestTenants()
	if len(testTenants) > 0 {
		kvDB = testTenants[0].DB()
		execCfg = testTenants[0].ExecutorConfig().(sql.ExecutorConfig)
	}

	var sessionData sessiondatapb.SessionData
	var sessionSerialized []byte
	tDB.QueryRow(t, "SELECT crdb_internal.serialize_session()").Scan(&sessionSerialized)
	require.NoError(t, protoutil.Unmarshal(sessionSerialized, &sessionData))

	getManagerForUser := func(u string) (eval.ReplicationStreamManager, error) {
		sqlUser, err := username.MakeSQLUsernameFromUserInput(u, username.PurposeValidation)
		require.NoError(t, err)
		txn := kvDB.NewTxn(ctx, "test")
		p, cleanup := sql.NewInternalPlanner("test", txn, sqlUser, &sql.MemoryMetrics{}, &execCfg, sessionData)

		// Extract
		pi := p.(interface {
			EvalContext() *eval.Context
			InternalSQLTxn() descs.Txn
		})
		defer cleanup()
		ec := pi.EvalContext()
		return newReplicationStreamManagerWithPrivilegesCheck(ctx, ec, pi.InternalSQLTxn())
	}

	tDB.Exec(t, "CREATE ROLE somebody")
	tDB.Exec(t, "GRANT SYSTEM REPLICATION TO somebody")

	for _, tc := range []struct {
		user         string
		expErr       string
		isEnterprise bool
	}{
		{user: "admin", expErr: "", isEnterprise: true},
		{user: "root", expErr: "", isEnterprise: true},
		{user: "somebody", expErr: "", isEnterprise: true},
		{user: "nobody", expErr: "user nobody does not have REPLICATION system privilege", isEnterprise: true},

		{user: "admin", expErr: "", isEnterprise: false},
		{user: "root", expErr: "", isEnterprise: false},
		{user: "somebody", expErr: "", isEnterprise: false},
		{user: "nobody", expErr: "user nobody does not have REPLICATION system privilege", isEnterprise: false},
	} {
		t.Run(fmt.Sprintf("%s/ent=%t", tc.user, tc.isEnterprise), func(t *testing.T) {
			if tc.isEnterprise {
				defer utilccl.TestingEnableEnterprise()()
			} else {
				defer utilccl.TestingDisableEnterprise()()
			}

			m, err := getManagerForUser(tc.user)
			if tc.expErr == "" {
				require.NoError(t, err)
				require.NotNil(t, m)
			} else {
				require.Regexp(t, tc.expErr, err)
				require.Nil(t, m)
			}
		})
	}

}
