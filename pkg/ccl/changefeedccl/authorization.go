// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"net/url"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsauth"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlclustersettings"
	"github.com/cockroachdb/errors"
)

func checkPrivilegesForDescriptor(
	ctx context.Context, p sql.PlanHookState, desc catalog.Descriptor,
) (hasSelect bool, hasChangefeed bool, err error) {
	if desc.GetObjectType() != privilege.Table {
		return false, false, errors.AssertionFailedf("expected descriptor %d to be a table descriptor, found: %s ", desc.GetID(), desc.GetObjectType())
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
//   - A sink the node authenticates to implicitly (rather than with credentials the
//     statement supplied) requires privilege.EXTERNALIOIMPLICITACCESS.
func authorizeUserToCreateChangefeed(
	ctx context.Context,
	p sql.PlanHookState,
	sinkURI string,
	hasSelectPrivOnAllTables bool,
	hasChangefeedPrivOnAllTables bool,
	confluentSchemaRegistryURI string,
	permitLegacyCSRSecurityOptOut bool,
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
				`user %s requires the %s privilege on all target tables to be able to run an enterprise changefeed`,
				p.User(), privilege.CHANGEFEED)
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
	requireUsagePrivilege :=
		sqlclustersettings.RequireUsagePrivilegeOnExternalConnections.Get(&p.ExecCfg().Settings.SV)
	requireUsage := requireExternalConnectionSink || requireUsagePrivilege

	// applyExternalConnectionSinkToRegistry decides whether the
	// require_external_connection_sink cluster setting (which forces a URI to
	// be an external connection) also covers the schema registry URI.
	// Only ALTER CHANGEFEED does not enforce it there (it sets
	// permitLegacyCSRSecurityOptOut) unless RequireUsagePrivilegeOnExternalConnections
	// is on, which restores uniform treatment. CREATE CHANGEFEED can never opt out.
	applyExternalConnectionSinkToRegistry := !permitLegacyCSRSecurityOptOut || requireUsagePrivilege

	requireImplicitAccess := changefeedbase.RequireExternalIOImplicitAccess.Get(&p.ExecCfg().Settings.SV) &&
		!p.ExecCfg().ExternalIODirConfig.EnableNonAdminImplicitAndArbitraryOutbound

	// TODO(CRDB-66647): drop this early return along with the setting, at which
	// point USAGE is required unconditionally.
	if !requireUsage && !requireImplicitAccess {
		return nil
	}

	checkURI := func(uriString string, applyExternalConnectionSink bool) error {
		if uriString == "" {
			return nil
		}
		uri, err := url.Parse(uriString)
		if err != nil {
			return errors.Newf("failed to parse url %s", uriString)
		}

		if uri.Scheme == changefeedbase.SinkSchemeExternalConnection {
			if requireUsage {
				return sql.CheckDestinationPrivilegesWithOptions(ctx, p, []string{uriString},
					sql.DestinationPrivilegeOptions{EnforceUsage: true})
			}
			return nil
		}

		if applyExternalConnectionSink && requireExternalConnectionSink {
			return pgerror.Newf(pgcode.InsufficientPrivilege,
				`the %s privilege on all tables can only be used with external connection sinks. see cluster setting %s`,
				privilege.CHANGEFEED, changefeedbase.RequireExternalConnectionSink.Name())
		}

		if requireImplicitAccess {
			return sql.CheckDestinationPrivilegesWithOptions(ctx, p, []string{uriString},
				sql.DestinationPrivilegeOptions{
					EnforceImplicitAccess: true,
					Resolver:              resolveSinkDestination,
				})
		}
		return nil
	}

	if err := checkURI(confluentSchemaRegistryURI, applyExternalConnectionSinkToRegistry); err != nil {
		return err
	}
	return checkURI(sinkURI, true /* applyExternalConnectionSink */)
}

// AuthorizeChangefeedJobAccess determines if a user has access to the changefeed job denoted
// by the supplied jobID and payload.
func AuthorizeChangefeedJobAccess(
	ctx context.Context,
	a jobsauth.AuthorizationAccessor,
	jobID jobspb.JobID,
	getLegacyPayload func(ctx context.Context) (*jobspb.Payload, error),
) error {
	payload, err := getLegacyPayload(ctx)
	if err != nil {
		return err
	}
	specs, ok := payload.UnwrapDetails().(jobspb.ChangefeedDetails)
	if !ok {
		return errors.Newf("could not unwrap details from the payload of job %d", jobID)
	}

	if len(specs.TargetSpecifications) == 0 {
		return pgerror.Newf(pgcode.InsufficientPrivilege, "job contains no tables on which the user has %s privilege", privilege.CHANGEFEED)
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

// externalSink is the ExternalDestination for a changefeed-native sink scheme
// (e.g. kafka://, pulsar://, webhook-*://, ...) that has no ExternalStorage
// representation.
type externalSink struct {
	scheme string
}

func (s externalSink) AccessIsWithExplicitAuth() bool { return false }
func (s externalSink) Kind() string                   { return s.scheme }

var _ cloudpb.ExternalDestination = externalSink{}

// resolveSinkDestination maps a changefeed sink URI to the ExternalDestination
// whose auth posture the implicit-access check reasons about.
// NB: All externalSinks (changefeed-only destinations) will always be considered
// to be using implicit-access.
func resolveSinkDestination(
	uriString string, user username.SQLUsername,
) (cloudpb.ExternalDestination, error) {
	uri, err := url.Parse(uriString)
	if err != nil {
		return nil, errors.Newf("failed to parse url %s", uriString)
	}

	if uri.Scheme == changefeedbase.SinkSchemeNull {
		return &cloudpb.ExternalStorage{Provider: cloudpb.ExternalStorageProvider_null}, nil
	}

	normalized := *uri
	normalized.Scheme = strings.TrimPrefix(normalized.Scheme, `experimental-`)
	normalized.Scheme = strings.TrimPrefix(normalized.Scheme, `file-`)
	if isCloudStorageSink(&normalized) {
		query := normalized.Query()
		query.Del(changefeedbase.SinkParamFileSize)
		query.Del(changefeedbase.SinkParamPartitionFormat)
		normalized.RawQuery = query.Encode()

		conf, err := cloud.ExternalStorageConfFromURI(normalized.String(), user)
		if err != nil {
			return nil, errors.Wrap(err, "resolving cloud storage sink destination")
		}
		return &conf, nil
	}

	return externalSink{scheme: uri.Scheme}, nil
}
