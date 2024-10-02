// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

	// UserHasRoleOption mirrors sql.AuthorizationAccessor.
	UserHasRoleOption(ctx context.Context, user username.SQLUsername, roleOption roleoption.Option) (bool, error)

	// HasRoleOption mirrors sql.AuthorizationAccessor.
	HasRoleOption(ctx context.Context, roleOption roleoption.Option) (bool, error)

	// UserHasAdminRole mirrors sql.AuthorizationAccessor.
	UserHasAdminRole(ctx context.Context, user username.SQLUsername) (bool, error)

	// HasAdminRole mirrors sql.AuthorizationAccessor.
	HasAdminRole(ctx context.Context) (bool, error)

	// HasGlobalPrivilegeOrRoleOption mirrors sql.AuthorizationAccessor.
	HasGlobalPrivilegeOrRoleOption(ctx context.Context, privilege privilege.Kind) (bool, error)

	// User mirrors sql.PlanHookState.
	User() username.SQLUsername
}

// GlobalJobPrivileges is used in conjunction with GetGlobalJobPrivileges to
// avoid unnecessary work in Authorize.
type GlobalJobPrivileges struct {
	// hasControl and hasView indicate whether the current user has the global
	// CONTROLJOB and VIEWJOB privileges respectively.
	hasControl, hasView bool
}

// GetGlobalJobPrivileges is a helper used to check whether the current user
// has the global CONTROLJOB and VIEWJOB privileges respectively. It is separate
// from Authorize so that it can be called once, and its result repeatedly
// passed to Authorize.
func GetGlobalJobPrivileges(
	ctx context.Context, a AuthorizationAccessor,
) (GlobalJobPrivileges, error) {
	hasControlJob, err := a.HasGlobalPrivilegeOrRoleOption(ctx, privilege.CONTROLJOB)
	if err != nil {
		return GlobalJobPrivileges{}, err
	}
	hasViewJob, err := a.HasGlobalPrivilegeOrRoleOption(ctx, privilege.VIEWJOB)
	if err != nil {
		return GlobalJobPrivileges{}, err
	}
	return GlobalJobPrivileges{hasControl: hasControlJob, hasView: hasViewJob}, nil
}

// Authorize returns nil if the user is authorized to access the job.
// If the user is not authorized, then a pgcode.InsufficientPrivilege
// error will be returned.
//
// TODO(#96432): sort out internal job owners and rules for accessing them
// Authorize checks these rules in order:
//  1. If the user is an admin, grant access.
//  2. If the AccessLevel is ViewAccess, grant access if the user has CONTROLJOB, VIEWJOB,
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
	global GlobalJobPrivileges,
) error {
	callerIsAdmin, err := a.UserHasAdminRole(ctx, a.User())
	if err != nil {
		return err
	}
	if callerIsAdmin {
		return nil
	}

	jobOwnerUser := payload.UsernameProto.Decode()

	if accessLevel == ViewAccess {
		if global.hasControl || global.hasView || a.User() == jobOwnerUser {
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
	if global.hasControl {
		return nil
	}

	typ := payload.Type()
	if check, ok := authorizers[typ]; ok {
		return check(ctx, a, jobID, payload)
	}
	return pgerror.Newf(pgcode.InsufficientPrivilege,
		"user %s does not have privileges for job %d",
		a.User(), jobID)
}
