// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"net/url"
	"slices"

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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlclustersettings"
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
//     If changefeedbase.RequireExternalConnectionSink is enabled, every URI it names must
//     be an external connection.
//   - Every external connection the changefeed names, as its sink or otherwise, requires
//     privilege.USAGE on it, CONTROLCHANGEFEED users included.
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

	requiredPrivilegeTarget := "all target tables"
	if changefeedLevel == tree.ChangefeedLevelDatabase {
		requiredPrivilegeTarget = "the target database"
	}

	isCore := sinkURI == ""
	switch {
	case hasControlChangefeed:
		if !hasSelectPrivOnAllTables {
			return pgerror.Newf(pgcode.InsufficientPrivilege,
				"user %s with %s role option requires the %s privilege on all target tables to be able to run an enterprise changefeed",
				p.User(), roleoption.CONTROLCHANGEFEED, privilege.SELECT)
		}
		p.BufferClientNotice(ctx, pgnotice.Newf("You are creating a changefeed as a user with the %s role option. %s",
			roleoption.CONTROLCHANGEFEED, roleoption.ControlChangefeedDeprecationNoticeMsg))
	case isCore:
		if !hasSelectPrivOnAllTables {
			return pgerror.Newf(pgcode.InsufficientPrivilege,
				`user %s requires the %s privilege on all target tables to be able to run a core changefeed`,
				p.User(), privilege.SELECT)
		}
	default:
		if !hasChangefeedPrivOnAllTables {
			return pgerror.Newf(pgcode.InsufficientPrivilege,
				`user %q requires the %s privilege on %s to be able to run an enterprise changefeed`,
				p.User(), privilege.CHANGEFEED, requiredPrivilegeTarget)
		}
	}

	// RequireExternalConnectionSink constrains where an enterprise changefeed may
	// send data. It has nothing to say about a core changefeed, nor about a
	// CONTROLCHANGEFEED user, who may use any sink.
	requireExternalConnectionSink := !isCore && !hasControlChangefeed &&
		changefeedbase.RequireExternalConnectionSink.Get(&p.ExecCfg().Settings.SV)

	// Confining a changefeed to external connection sinks carries a USAGE
	// requirement on the connections it names, so either setting turns the
	// check on. This preserves the pre-v26.4 behavior, where a changefeed
	// confined to external connection sinks always required USAGE on them.
	requireUsage := requireExternalConnectionSink ||
		sqlclustersettings.RequireUsagePrivilegeOnExternalConnections.Get(&p.ExecCfg().Settings.SV)

	// TODO(CRDB-66647): drop this early return along with the setting, at which
	// point USAGE is required unconditionally.
	if !requireUsage {
		return nil
	}

	for _, uriString := range slices.Concat(otherExternalURIs, []string{sinkURI}) {
		if uriString == "" {
			continue
		}
		isExternalConnection, err := checkExternalConnectionUsage(ctx, p, uriString)
		if err != nil {
			return err
		}
		if !isExternalConnection && requireExternalConnectionSink {
			return pgerror.Newf(pgcode.InsufficientPrivilege,
				`the %s privilege on %s can only be used with external connection sinks. see cluster setting %s`,
				privilege.CHANGEFEED, requiredPrivilegeTarget,
				changefeedbase.RequireExternalConnectionSink.Name())
		}
	}

	return nil
}

// checkExternalConnectionUsage requires privilege.USAGE on the external
// connection uriString names, and reports whether it named one at all.
func checkExternalConnectionUsage(
	ctx context.Context, p sql.PlanHookState, uriString string,
) (isExternalConnection bool, _ error) {
	uri, err := url.Parse(uriString)
	if err != nil {
		return false, errors.Newf("failed to parse url %s", uriString)
	}
	if uri.Scheme != changefeedbase.SinkSchemeExternalConnection {
		return false, nil
	}

	ec, err := externalconn.LoadExternalConnection(ctx, uri.Host, p.InternalSQLTxn())
	if err != nil {
		return true, errors.Wrap(err, "failed to load external connection object")
	}
	ecPriv := &syntheticprivilege.ExternalConnectionPrivilege{
		ConnectionName: ec.ConnectionName(),
	}
	return true, p.CheckPrivilege(ctx, ecPriv, privilege.USAGE)
}
