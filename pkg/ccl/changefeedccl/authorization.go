// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/errors"
)

func checkPrivilegesForDescriptor(
	ctx context.Context, p sql.PlanHookState, desc catalog.Descriptor,
) (hasSelect bool, hasChangefeed bool, err error) {
	if desc == nil {
		return false, false, errors.AssertionFailedf("expected descriptor to be non-nil")
	}
	switch desc.GetObjectType() {
	case privilege.Database, privilege.Table:
	default:
		return false, false, errors.AssertionFailedf(
			"expected descriptor for %q to be a table or database descriptor, found: %s ",
			desc.GetName(),
			desc.GetObjectType(),
		)
	}

	hasSelect, err = p.HasPrivilege(ctx, desc, privilege.SELECT, p.User())
	if err != nil {
		return false, false, err
	}

	hasChangefeed, err = p.HasPrivilege(ctx, desc, privilege.CHANGEFEED, p.User())
	if err != nil {
		return false, false, err
	}

	return hasSelect, hasChangefeed, nil
}

// authorizeUserToCreateChangefeed performs changefeed creation authorization checks, returning a
// pgcode.InsufficientPrivilege error if the check fails.
//
// TODO(#94757): remove CONTROLCHANGEFEED entirely
// Admins can create any kind of changefeed. For non-admins:
//   - The first check which is performed is checking if a user has CONTROLCHANGEFEED. If so,
//     we enforce that they require privilege.SELECT on all target tables. Such as user
//     can use any sink.
//   - To create a core changefeed, a user requires privilege.SELECT on all targeted tables.
//   - To create an enterprise changefeed, the user requires privilege.CHANGEFEED on all tables.
//     If changefeedbase.RequireExternalConnectionSink is enabled, then the changefeed
//     must be used with an external connection and the user requires privilege.USAGE on it.
func authorizeUserToCreateChangefeed(
	ctx context.Context,
	p sql.PlanHookState,
	sinkURI string,
	hasSelectPrivOnAllTables bool,
	hasChangefeedPrivOnAllTables bool,
	changefeedLevel tree.ChangefeedLevel,
	otherExternalURIs ...string,
) error {
	isAdmin, err := p.HasAdminRole(ctx)
	if err != nil {
		return err
	}
	if isAdmin {
		return nil
	}

	hasControlChangefeed, err := p.HasRoleOption(ctx, roleoption.CONTROLCHANGEFEED)
	if err != nil {
		return err
	}
	if hasControlChangefeed {
		if !hasSelectPrivOnAllTables {
			return pgerror.Newf(pgcode.InsufficientPrivilege,
				"user %s with %s role option requires the %s privilege on all target tables to be able to run an enterprise changefeed",
				p.User(), roleoption.CONTROLCHANGEFEED, privilege.SELECT)
		}
		p.BufferClientNotice(ctx, pgnotice.Newf("You are creating a changefeed as a user with the %s role option. %s",
			roleoption.CONTROLCHANGEFEED, roleoption.ControlChangefeedDeprecationNoticeMsg))
		return nil
	}

	if sinkURI == "" {
		if !hasSelectPrivOnAllTables {
			return pgerror.Newf(pgcode.InsufficientPrivilege,
				`user %s requires the %s privilege on all target tables to be able to run a core changefeed`,
				p.User(), privilege.SELECT)
		}
		return nil
	}

	requiredPrivilegeTarget := func() string {
		if changefeedLevel == tree.ChangefeedLevelDatabase {
			return "the target database"
		}
		return "all target tables"
	}()

	if !hasChangefeedPrivOnAllTables {
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			`user %q requires the %s privilege on %s to be able to run an enterprise changefeed`,
			p.User(), privilege.CHANGEFEED, requiredPrivilegeTarget)
	}

	enforceExternalConnections := changefeedbase.RequireExternalConnectionSink.Get(&p.ExecCfg().Settings.SV)
	if enforceExternalConnections {
		for _, uriString := range append(otherExternalURIs, sinkURI) {
			if uriString == "" {
				continue
			}
			uri, err := url.Parse(uriString)
			if err != nil {
				return errors.Newf("failed to parse url %s", uriString)
			}
			if uri.Scheme == changefeedbase.SinkSchemeExternalConnection {
				ec, err := externalconn.LoadExternalConnection(ctx, uri.Host, p.InternalSQLTxn())
				if err != nil {
					return errors.Wrap(err, "failed to load external connection object")
				}
				ecPriv := &syntheticprivilege.ExternalConnectionPrivilege{
					ConnectionName: ec.ConnectionName(),
				}
				if err := p.CheckPrivilege(ctx, ecPriv, privilege.USAGE); err != nil {
					return err
				}
			} else {
				return pgerror.Newf(
					pgcode.InsufficientPrivilege,
					`the %s privilege on %s can only be used with external connection sinks. see cluster setting %s`,
					privilege.CHANGEFEED, requiredPrivilegeTarget, changefeedbase.RequireExternalConnectionSink.Name(),
				)
			}
		}
	}

	return nil
}
