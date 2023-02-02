// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsauth"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/errors"
)

func checkPrivilegesForDescriptor(
	ctx context.Context, p sql.PlanHookState, desc catalog.Descriptor,
) (hasSelect bool, hasChangefeed bool, err error) {
	if desc.GetObjectType() != privilege.Table {
		return false, false, errors.AssertionFailedf("expected descriptor %d to be a table descriptor. instead found: %s ", desc.GetID(), desc.GetObjectType())
	}

	hasSelect, hasChangefeed = true, true
	if err = p.CheckPrivilege(ctx, desc, privilege.SELECT); err != nil {
		if !sql.IsInsufficientPrivilegeError(err) {
			return false, false, err
		}
		hasSelect = false
	}
	if err = p.CheckPrivilege(ctx, desc, privilege.CHANGEFEED); err != nil {
		if !sql.IsInsufficientPrivilegeError(err) {
			return false, false, err
		}
		hasChangefeed = false
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

	if !hasChangefeedPrivOnAllTables {
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			`user %s requires the %s privilege on all target tables to be able to run an enterprise changefeed`,
			p.User(), privilege.CHANGEFEED)
	}

	enforceExternalConnections := changefeedbase.RequireExternalConnectionSink.Get(&p.ExecCfg().Settings.SV)
	if enforceExternalConnections {
		uri, err := url.Parse(sinkURI)
		if err != nil {
			return errors.Newf("failed to parse url %s", sinkURI)
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
				`the %s privilege on all tables can only be used with external connection sinks. see cluster setting %s`,
				privilege.CHANGEFEED, changefeedbase.RequireExternalConnectionSink.Key(),
			)
		}
	}

	return nil
}

// AuthorizeChangefeedJobAccess determines if a user has access to the changefeed job denoted
// by the supplied jobID and payload.
func AuthorizeChangefeedJobAccess(
	ctx context.Context,
	a jobsauth.AuthorizationAccessor,
	jobID jobspb.JobID,
	payload *jobspb.Payload,
) error {
	specs, ok := payload.UnwrapDetails().(jobspb.ChangefeedDetails)
	if !ok {
		return errors.Newf("could not unwrap details from the payload of job %d", jobID)
	}

	for _, spec := range specs.TargetSpecifications {
		err := a.CheckPrivilegeForTableID(ctx, spec.TableID, privilege.CHANGEFEED)
		if err != nil {
			// When performing SHOW JOBS or SHOW CHANGEFEED JOBS, there may be old changefeed
			// records that reference tables which have been dropped or are being
			// dropped. In this case, we would prefer to skip the permissions check on
			// the dropped descriptor.
			if pgerror.GetPGCode(err) == pgcode.UndefinedTable || errors.Is(err, catalog.ErrDescriptorDropped) {
				continue
			}

			return err
		}
	}
	return nil
}

func init() {
	jobsauth.RegisterAuthorizer(jobspb.TypeChangefeed, AuthorizeChangefeedJobAccess)
}
