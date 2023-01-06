package privilege

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/errors"
)

// AuthorizationAccessor for checking authorization on jobs.
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
	ctx context.Context, a AuthorizationAccessor, payload *jobspb.Payload,
) (canAccess bool, userErr error, err error) {
	specs := payload.UnwrapDetails().(jobspb.ChangefeedDetails).TargetSpecifications

	for _, spec := range specs {
		err := a.CheckPrivilegeForTableID(ctx, spec.TableID, privilege.CHANGEFEED)

		if err != nil {
			// Return the privilege error as a user error.
			if pgerror.GetPGCode(err) == pgcode.InsufficientPrivilege {
				return false, err, nil
			}

			// When performing SHOW JOBS or SHOW CHANGEFEED JOBS, there may be old changefeed
			// records that reference tables which have been dropped or are being
			// dropped. In this case, we would prefer to skip the permissions check on
			// the dropped descriptor.
			if pgerror.GetPGCode(err) == pgcode.UndefinedTable || errors.Is(err, catalog.ErrDescriptorDropped) {
				continue
			}

			return false, nil, err
		}
	}
	return true, nil, nil
}

// JobTypeSpecificPrivilegeCheck returns true if the user should be able to access
// the job. If the returned value is false and err is nil, then userErr will be
// returned with an appropriate error that can be passed up to the user.
// allowSameUserAccess specifies if users can access their own jobs.
func JobTypeSpecificPrivilegeCheck(
	ctx context.Context,
	a AuthorizationAccessor,
	jobID jobspb.JobID,
	payload *jobspb.Payload,
	allowSameUserAccess bool,
) (canAccess bool, userErr error, err error) {
	userIsAdmin, err := a.HasAdminRole(ctx)
	if err != nil {
		return false, nil, err
	}

	userHasControlJob, err := a.HasRoleOption(ctx, roleoption.CONTROLJOB)
	if err != nil {
		return false, nil, err
	}

	jobOwnerUser := payload.UsernameProto.Decode()
	jobOwnerIsAdmin, err := a.UserHasAdminRole(ctx, jobOwnerUser)
	if err != nil {
		return false, nil, err
	}

	if jobOwnerIsAdmin {
		if !userIsAdmin {
			return false, pgerror.Newf(pgcode.InsufficientPrivilege,
				"only admins can control jobs owned by other admins"), nil
		}
		return true, nil, nil
	}

	fmt.Println(a.User(), userHasControlJob, allowSameUserAccess, jobOwnerUser)
	if userHasControlJob || (allowSameUserAccess && a.User() == jobOwnerUser) {
		return true, nil, nil
	}

	switch payload.Type() {
	case jobspb.TypeChangefeed:
		return changefeedPrivilegeCheck(ctx, a, payload)
	default:
		return false, pgerror.Newf(pgcode.InsufficientPrivilege,
			"user %s does not have %s privilege for job $d",
			a.User(), roleoption.CONTROLJOB, jobID), nil
	}
}
