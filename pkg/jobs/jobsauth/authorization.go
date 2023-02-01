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
)

// An AccessLevel is used to indicate how strict an authorization check should
// be.
type AccessLevel int

const (
	// ViewAccess is used to perform authorization for viewing jobs (ex. SHOW JOBS).
	ViewAccess AccessLevel = iota

	// ControlAccess is used to perform authorization for modifying jobs (ex. PAUSE|CANCEL|RESUME JOB).
	// This access level performs stricter checks than ViewAccess.
	//
	// The set of jobs visible via ControlAccess is a subset of jobs visible via
	// ViewAccess. In other words: if a user with a given set of privileges is
	// authorized to modify a job using ControlAccess, they will be authorized to
	// view it using ViewAccess.
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
// Authorization follows these rules in order:
//  1. If the user is an admin, grant access.
//  2. If the AccessLevel is ViewAccess, grant access if the user has CONTROLJOB
//     or if the user owns the job.
//  3. If the AccessLevel is ControlAccess, grant access if the user has CONTROLJOB
//     and the job owner is not "node" or "root". This prevents users from modifying
//     internal jobs (migrations, stats, schema changes etc.).
//  4. Perform job-type specific checks.
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

	jobOwnerUser := payload.UsernameProto.Decode()

	if accessLevel == ViewAccess {
		if a.User() == jobOwnerUser || hasControlJob {
			return nil
		}
	}

	ownedByRootOrNode := jobOwnerUser.IsNodeUser() || jobOwnerUser.IsRootUser()
	if ownedByRootOrNode {
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"only admins can control jobs owned by %s or %s", username.NodeUser, username.RootUser)
	}
	if hasControlJob {
		return nil
	}

	typ := payload.Type()
	if check, ok := authorizers[typ]; ok {
		return check(ctx, a, jobID, payload)
	}
	return pgerror.Newf(pgcode.InsufficientPrivilege,
		"user %s does not have %s privilege for job $d",
		a.User(), roleoption.CONTROLJOB, jobID)
}
