// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobsauth

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
)

// An AccessLevel is used to indicate how strict an authorization check should
// be.
type AccessLevel int

const (
	// ViewAccess is used to perform authorization for viewing jobs (ex. SHOW JOBS).
	ViewAccess AccessLevel = iota

	// ControlAccess is used to perform authorization for modifying jobs (ex. PAUSE|CANCEL|RESUME JOB).
	ControlAccess
)

var authorizers = make(map[jobspb.Type]Authorizer)

// Authorizer is a function which returns a pgcode.InsufficientPrivilege error if
// authorization for the job denoted by jobID and payload fails.
type Authorizer func(
	ctx context.Context, a AuthorizationAccessor, jobID jobspb.JobID, payload *jobspb.Payload,
) error

// RegisterAuthorizer registers a AuthorizationCheck for a certain job type.
func RegisterAuthorizer(typ jobspb.Type, fn Authorizer) {
	if _, ok := authorizers[typ]; ok {
		panic(fmt.Sprintf("cannot register two authorizers for the type %s", typ))
	}

	authorizers[typ] = fn
}

// AuthorizationAccessor is an interface for checking authorization on jobs.
type AuthorizationAccessor interface {
	// CheckPrivilegeForTableID mirrors sql.AuthorizationAccessor.
	CheckPrivilegeForTableID(ctx context.Context, tableID descpb.ID, privilege privilege.Kind) error

	// HasPrivilege mirrors sql.AuthorizationAccessor.
	HasPrivilege(ctx context.Context, privilegeObject privilege.Object, privilege privilege.Kind, user username.SQLUsername) (bool, error)

	// HasRoleOption mirrors sql.AuthorizationAccessor.
	HasRoleOption(ctx context.Context, roleOption roleoption.Option) (bool, error)

	// UserHasAdminRole mirrors sql.AuthorizationAccessor.
	UserHasAdminRole(ctx context.Context, user username.SQLUsername) (bool, error)

	// HasAdminRole mirrors sql.AuthorizationAccessor.
	HasAdminRole(ctx context.Context) (bool, error)

	// User mirrors sql.PlanHookState.
	User() username.SQLUsername
}

// Authorize returns nil if the user is authorized to access the job.
// If the user is not authorized, then a pgcode.InsufficientPrivilege
// error will be returned.
//
// TODO(#96432): sort out internal job owners and rules for accessing them
// Authorize checks these rules in order:
//  1. If the user is an admin, grant access.
//  2. If the AccessLevel is ViewAccess, grant access if the user has CONTROLJOB
//     or if the user owns the job.
//  3. If the AccessLevel is ControlAccess, grant access if the user has CONTROLJOB
//     and the job owner is not an admin.
//  4. If there is an authorization check for this job type that passes, grant the user access.
func Authorize(
	ctx context.Context,
	a AuthorizationAccessor,
	jobID jobspb.JobID,
	payload *jobspb.Payload,
	accessLevel AccessLevel,
) error {
	userIsAdmin, err := a.HasAdminRole(ctx)
	if err != nil {
		return err
	}
	if userIsAdmin {
		return nil
	}

	hasControlJob, err := a.HasRoleOption(ctx, roleoption.CONTROLJOB)
	if err != nil {
		return err
	}

	hasViewJob, err := a.HasPrivilege(ctx, &syntheticprivilege.GlobalPrivilege{}, privilege.VIEWJOB, a.User())
	if err != nil {
		return err
	}

	jobOwnerUser := payload.UsernameProto.Decode()

	if accessLevel == ViewAccess {
		if a.User() == jobOwnerUser || hasControlJob || hasViewJob {
			return nil
		}
	}

	jobOwnerIsAdmin, err := a.UserHasAdminRole(ctx, jobOwnerUser)
	if err != nil {
		return err
	}
	if jobOwnerIsAdmin {
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"only admins can control jobs owned by other admins")
	}
	if hasControlJob {
		return nil
	}

	typ := payload.Type()
	if check, ok := authorizers[typ]; ok {
		return check(ctx, a, jobID, payload)
	}
	return pgerror.Newf(pgcode.InsufficientPrivilege,
		"user %s does not have privileges for job $d",
		a.User(), jobID)
}
