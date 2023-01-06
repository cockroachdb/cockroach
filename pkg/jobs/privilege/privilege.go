// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package privilege

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
)

// An AccessFlag is used to if a special rule should be applied
// when determining if a user can access a job.
type AccessFlag int

const (
	// UserCanAccessOwnJob permits users to access jobs if they are the job owner.
	UserCanAccessOwnJob AccessFlag = iota
	// UserCanAccessWithControlJob permits users to use roleoption.CONTROLJOB
	// to see jobs owned by other users that are not admins.
	UserCanAccessWithControlJob
)

type accessFlagSet struct {
	inner intsets.Fast
}

func (s *accessFlagSet) contains(flag AccessFlag) bool {
	return s.inner.Contains(int(flag))
}

func makeAccessFlagSet(flags []AccessFlag) accessFlagSet {
	flagsSet := intsets.MakeFast()
	for _, f := range flags {
		flagsSet.Add(int(f))
	}
	return accessFlagSet{inner: flagsSet}
}

// AuthorizationAccessor is an interface for checking authorization on jobs.
type AuthorizationAccessor interface {
	// CheckPrivilegeForTableID mirrors sql.AuthorizationAccessor.
	CheckPrivilegeForTableID(ctx context.Context, tableID descpb.ID, privilege privilege.Kind) error

	// HasRoleOption mirrors sql.AuthorizationAccessor.
	HasRoleOption(ctx context.Context, roleOption roleoption.Option) (bool, error)

	// UserHasAdminRole mirrors sql.AuthorizationAccessor.
	UserHasAdminRole(ctx context.Context, user username.SQLUsername) (bool, error)

	// HasAdminRole mirrors sql.AuthorizationAccessor.
	HasAdminRole(ctx context.Context) (bool, error)

	// User mirrors sql.PlanHookState.
	User() username.SQLUsername
}

// changefeedPrivilegeCheck determines if a user has access to the changefeed defined
// by the supplied payload.
func changefeedPrivilegeCheck(
	ctx context.Context, a AuthorizationAccessor, specs []jobspb.ChangefeedTargetSpecification,
) error {

	for _, spec := range specs {
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

// Authorize returns true if the user should be able to access
// the job. If the returned value is false and err is nil, then userErr will be
// returned with an appropriate error that can be passed up to the user.
// allowSameUserAccess specifies if users can access their own jobs.
func Authorize(
	ctx context.Context,
	a AuthorizationAccessor,
	jobID jobspb.JobID,
	payload *jobspb.Payload,
	accessFlags ...AccessFlag,
) error {
	flags := makeAccessFlagSet(accessFlags)

	userIsAdmin, err := a.HasAdminRole(ctx)
	if err != nil {
		return err
	}

	userHasControlJob, err := a.HasRoleOption(ctx, roleoption.CONTROLJOB)
	if err != nil {
		return err
	}

	jobOwnerUser := payload.UsernameProto.Decode()
	jobOwnerIsAdmin, err := a.UserHasAdminRole(ctx, jobOwnerUser)
	if err != nil {
		return err
	}

	if jobOwnerIsAdmin {
		if !userIsAdmin {
			return pgerror.Newf(pgcode.InsufficientPrivilege,
				"only admins can control jobs owned by other admins")
		}
		return nil
	}

	if (flags.contains(UserCanAccessWithControlJob) && userHasControlJob) || (flags.contains(UserCanAccessOwnJob) && a.User() == jobOwnerUser) {
		return nil
	}

	switch payload.Type() {
	case jobspb.TypeChangefeed:
		specs, ok := payload.UnwrapDetails().(jobspb.ChangefeedDetails)
		if !ok {
			return errors.Newf("could not unwrap details from the payload of job %d."+
				" consider deleting this job from system.jobs", jobID)
		}

		return changefeedPrivilegeCheck(ctx, a, specs.TargetSpecifications)
	default:
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"user %s does not have %s privilege for job $d",
			a.User(), roleoption.CONTROLJOB, jobID)
	}
}
