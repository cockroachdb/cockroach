// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamproducer

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func TestReplicationManagerRequiresAdminRole(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	tDB := sqlutils.MakeSQLRunner(sqlDB)

	var sessionData sessiondatapb.SessionData
	{
		var sessionSerialized []byte
		tDB.QueryRow(t, "SELECT crdb_internal.serialize_session()").Scan(&sessionSerialized)
		require.NoError(t, protoutil.Unmarshal(sessionSerialized, &sessionData))
	}

	getManagerForUser := func(u string) (eval.ReplicationStreamManager, error) {
		sqlUser, err := username.MakeSQLUsernameFromUserInput(u, username.PurposeValidation)
		require.NoError(t, err)
		execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
		txn := kvDB.NewTxn(ctx, "test")
		p, cleanup := sql.NewInternalPlanner("test", txn, sqlUser, &sql.MemoryMetrics{}, &execCfg, sessionData)
		defer cleanup()
		ec := p.(interface{ EvalContext() *eval.Context }).EvalContext()
		var man eval.ReplicationStreamManager
		if err := execCfg.InternalDB.Txn(ctx, func(
			ctx context.Context, txn isql.Txn,
		) (err error) {
			man, err = newReplicationStreamManagerWithPrivilegesCheck(ctx, ec, txn)
			return err
		}); err != nil {
			return nil, err
		}
		return man, nil
	}

	for _, tc := range []struct {
		user         string
		expErr       string
		isEnterprise bool
	}{
		{user: "admin", expErr: "", isEnterprise: true},
		{user: "root", expErr: "", isEnterprise: true},
		{user: "nobody", expErr: "replication restricted to ADMIN role", isEnterprise: true},
		{user: "admin", expErr: "use of REPLICATION requires an enterprise license", isEnterprise: false},
		{user: "root", expErr: "use of REPLICATION requires an enterprise license", isEnterprise: false},
		{user: "nobody", expErr: "replication restricted to ADMIN role", isEnterprise: false},
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
