// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloudprivilege

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
)

// CheckDestinationPrivileges iterates over the External Storage URIs and
// ensures the user has adequate privileges to use each of them.
func CheckDestinationPrivileges(ctx context.Context, p sql.PlanHookState, to []string) error {
	isAdmin, err := p.UserHasAdminRole(ctx, p.User())
	if err != nil {
		return err
	}
	if isAdmin {
		return nil
	}

	// Check destination specific privileges.
	for _, uri := range to {
		conf, err := cloud.ExternalStorageConfFromURI(uri, p.User())
		if err != nil {
			return err
		}

		// Check if the destination requires the user to be an admin or have the
		// `EXTERNALIOIMPLICITACCESS` privilege.
		requiresImplicitAccess := !conf.AccessIsWithExplicitAuth()
		hasImplicitAccessPrivilege, privErr :=
			p.HasPrivilege(ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.EXTERNALIOIMPLICITACCESS, p.User())
		if privErr != nil {
			return privErr
		}
		if requiresImplicitAccess && !(p.ExecCfg().ExternalIODirConfig.EnableNonAdminImplicitAndArbitraryOutbound || hasImplicitAccessPrivilege) {
			return pgerror.Newf(
				pgcode.InsufficientPrivilege,
				"only users with the admin role or the EXTERNALIOIMPLICITACCESS system privilege are allowed to access the specified %s URI",
				conf.Provider.String())
		}

		// If the resource being used is an External Connection, check that the user
		// has adequate privileges.
		if conf.Provider == cloudpb.ExternalStorageProvider_external {
			if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.V22_2SystemExternalConnectionsTable) {
				return pgerror.Newf(pgcode.FeatureNotSupported,
					"version %v must be finalized to backup to an External Connection",
					clusterversion.ByKey(clusterversion.V22_2SystemExternalConnectionsTable))
			}
			ecPrivilege := &syntheticprivilege.ExternalConnectionPrivilege{
				ConnectionName: conf.ExternalConnectionConfig.Name,
			}
			if err := p.CheckPrivilege(ctx, ecPrivilege, privilege.USAGE); err != nil {
				return err
			}
		}
	}

	return nil
}
