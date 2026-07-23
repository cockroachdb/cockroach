// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package privchecker_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/privchecker"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestAdminPrivilegeChecker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	ts := s.ApplicationLayer()

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, "CREATE USER withadmin")
	sqlDB.Exec(t, "GRANT admin TO withadmin")
	sqlDB.Exec(t, "CREATE USER withva")
	sqlDB.Exec(t, "ALTER ROLE withva WITH VIEWACTIVITY")
	sqlDB.Exec(t, "CREATE USER withvaredacted")
	sqlDB.Exec(t, "ALTER ROLE withvaredacted WITH VIEWACTIVITYREDACTED")
	sqlDB.Exec(t, "CREATE USER withvaandredacted")
	sqlDB.Exec(t, "ALTER ROLE withvaandredacted WITH VIEWACTIVITY")
	sqlDB.Exec(t, "ALTER ROLE withvaandredacted WITH VIEWACTIVITYREDACTED")
	sqlDB.Exec(t, "CREATE USER withoutprivs")
	sqlDB.Exec(t, "CREATE USER withvaglobalprivilege")
	sqlDB.Exec(t, "GRANT SYSTEM VIEWACTIVITY TO withvaglobalprivilege")
	sqlDB.Exec(t, "CREATE USER withvaredactedglobalprivilege")
	sqlDB.Exec(t, "GRANT SYSTEM VIEWACTIVITYREDACTED TO withvaredactedglobalprivilege")
	sqlDB.Exec(t, "CREATE USER withvaandredactedglobalprivilege")
	sqlDB.Exec(t, "GRANT SYSTEM VIEWACTIVITY TO withvaandredactedglobalprivilege")
	sqlDB.Exec(t, "GRANT SYSTEM VIEWACTIVITYREDACTED TO withvaandredactedglobalprivilege")
	sqlDB.Exec(t, "CREATE USER withviewclustermetadata")
	sqlDB.Exec(t, "GRANT SYSTEM VIEWCLUSTERMETADATA TO withviewclustermetadata")
	sqlDB.Exec(t, "CREATE USER withviewdebug")
	sqlDB.Exec(t, "GRANT SYSTEM VIEWDEBUG TO withviewdebug")

	execCfg := ts.ExecutorConfig().(sql.ExecutorConfig)
	kvDB := ts.DB()

	plannerFn := func(opName redact.SafeString) (sql.AuthorizationAccessor, func()) {
		// This is a hack to get around a Go package dependency cycle. See comment
		// in sql/jobs/registry.go on planHookMaker.
		txn := kvDB.NewTxn(ctx, "test")
		p, cleanup := sql.NewInternalPlanner(
			opName,
			txn,
			username.NodeUserName(),
			&sql.MemoryMetrics{},
			&execCfg,
			sql.NewInternalSessionData(ctx, execCfg.Settings, opName),
		)
		return p.(sql.AuthorizationAccessor), cleanup
	}

	underTest := privchecker.NewChecker(
		ts.InternalExecutor().(isql.Executor),
		ts.ClusterSettings(),
	)

	underTest.SetAuthzAccessorFactory(plannerFn)

	withAdmin, err := username.MakeSQLUsernameFromPreNormalizedStringChecked("withadmin")
	require.NoError(t, err)
	withVa, err := username.MakeSQLUsernameFromPreNormalizedStringChecked("withva")
	require.NoError(t, err)
	withVaRedacted, err := username.MakeSQLUsernameFromPreNormalizedStringChecked("withvaredacted")
	require.NoError(t, err)
	withVaAndRedacted, err := username.MakeSQLUsernameFromPreNormalizedStringChecked("withvaandredacted")
	require.NoError(t, err)
	withoutPrivs, err := username.MakeSQLUsernameFromPreNormalizedStringChecked("withoutprivs")
	require.NoError(t, err)
	withVaGlobalPrivilege := username.MakeSQLUsernameFromPreNormalizedString("withvaglobalprivilege")
	withVaRedactedGlobalPrivilege := username.MakeSQLUsernameFromPreNormalizedString("withvaredactedglobalprivilege")
	withVaAndRedactedGlobalPrivilege := username.MakeSQLUsernameFromPreNormalizedString("withvaandredactedglobalprivilege")
	withviewclustermetadata := username.MakeSQLUsernameFromPreNormalizedString("withviewclustermetadata")
	withViewDebug := username.MakeSQLUsernameFromPreNormalizedString("withviewdebug")

	tests := []struct {
		name            string
		checkerFun      func(context.Context) error
		usernameWantErr map[username.SQLUsername]bool
	}{
		{
			"requireViewActivityPermission",
			underTest.RequireViewActivityPermission,
			map[username.SQLUsername]bool{
				withAdmin: false, withVa: false, withVaRedacted: true, withVaAndRedacted: false, withoutPrivs: true,
				withVaGlobalPrivilege: false, withVaRedactedGlobalPrivilege: true, withVaAndRedactedGlobalPrivilege: false,
			},
		},
		{
			"requireViewActivityOrViewActivityRedactedPermission",
			underTest.RequireViewActivityOrViewActivityRedactedPermission,
			map[username.SQLUsername]bool{
				withAdmin: false, withVa: false, withVaRedacted: false, withVaAndRedacted: false, withoutPrivs: true,
				withVaGlobalPrivilege: false, withVaRedactedGlobalPrivilege: false, withVaAndRedactedGlobalPrivilege: false,
			},
		},
		{
			"requireViewActivityAndNoViewActivityRedactedPermission",
			underTest.RequireViewActivityAndNoViewActivityRedactedPermission,
			map[username.SQLUsername]bool{
				withAdmin: false, withVa: false, withVaRedacted: true, withVaAndRedacted: true, withoutPrivs: true,
				withVaGlobalPrivilege: false, withVaRedactedGlobalPrivilege: true, withVaAndRedactedGlobalPrivilege: true,
			},
		},
		{
			"requireViewClusterMetadataPermission",
			underTest.RequireViewClusterMetadataPermission,
			map[username.SQLUsername]bool{
				withAdmin: false, withoutPrivs: true, withviewclustermetadata: false,
			},
		},
		{
			"requireViewDebugPermission",
			underTest.RequireViewDebugPermission,
			map[username.SQLUsername]bool{
				withAdmin: false, withoutPrivs: true, withViewDebug: false,
			},
		},
	}

	for _, tt := range tests {
		for userName, wantErr := range tt.usernameWantErr {
			t.Run(fmt.Sprintf("%s-%s", tt.name, userName), func(t *testing.T) {
				ctx := metadata.NewIncomingContext(ctx, metadata.New(map[string]string{"websessionuser": userName.SQLIdentifier()}))
				err := tt.checkerFun(ctx)
				if wantErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
			})
		}
	}
}
