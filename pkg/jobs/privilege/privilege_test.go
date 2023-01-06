// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package privilege_test

import (
	"context"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	jobsprivilege "github.com/cockroachdb/cockroach/pkg/jobs/privilege"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/assert"
)

type testAuthAccessor struct {
	user username.SQLUsername

	// set of role options that the user has
	roleOptions map[roleoption.Option]struct{}

	// set of descriptors which the user has privilege.CHANGEFEED on
	changeFeedPrivileges map[descpb.ID]struct{}
	// set of descriptors which are dropped
	droppedDescriptors map[descpb.ID]struct{}

	// set of all usernames who are admins
	admins map[string]struct{}

	rand *rand.Rand
}

var _ jobsprivilege.AuthorizationAccessor = &testAuthAccessor{}

func (a *testAuthAccessor) CheckPrivilegeForTableID(
	_ context.Context, tableID descpb.ID, _ privilege.Kind,
) error {
	if _, ok := a.droppedDescriptors[tableID]; ok {
		if a.rand.Int31n(2) == 0 {
			return catalog.ErrDescriptorDropped
		} else {
			return pgerror.New(pgcode.UndefinedTable, "foo")
		}
	}

	if _, ok := a.changeFeedPrivileges[tableID]; !ok {
		return pgerror.New(pgcode.InsufficientPrivilege, "foo")
	}
	return nil
}

func (a *testAuthAccessor) HasRoleOption(
	_ context.Context, roleOption roleoption.Option,
) (bool, error) {
	_, ok := a.roleOptions[roleOption]
	return ok, nil
}

func (a *testAuthAccessor) UserHasAdminRole(
	_ context.Context, user username.SQLUsername,
) (bool, error) {
	_, ok := a.admins[user.Normalized()]
	return ok, nil
}

func (a *testAuthAccessor) HasAdminRole(ctx context.Context) (bool, error) {
	_, ok := a.admins[a.user.Normalized()]
	return ok, nil
}

func (a *testAuthAccessor) User() username.SQLUsername {
	return a.user
}

func makePayload(
	owner string, changefeedSpecs []jobspb.ChangefeedTargetSpecification,
) *jobspb.Payload {
	var details interface{}
	details = jobspb.BackupDetails{}
	if len(changefeedSpecs) > 0 {
		details = jobspb.ChangefeedDetails{
			TargetSpecifications: changefeedSpecs,
		}
	}
	return &jobspb.Payload{
		Details:       jobspb.WrapPayloadDetails(details),
		UsernameProto: username.MakeSQLUsernameFromPreNormalizedString(owner).EncodeProto(),
	}
}

func makeChangefeedSpecs(tableIDs []descpb.ID) []jobspb.ChangefeedTargetSpecification {
	specs := make([]jobspb.ChangefeedTargetSpecification, len(tableIDs))
	for i, tableID := range tableIDs {
		specs[i] = jobspb.ChangefeedTargetSpecification{
			TableID: tableID,
		}
	}
	return specs
}

func TestPrivileges(t *testing.T) {
	rng, seed := randutil.NewTestRand()
	t.Logf("random seed: %d", seed)

	for _, tc := range []struct {
		name string

		user                 username.SQLUsername
		roleOptions          map[roleoption.Option]struct{}
		changeFeedPrivileges map[descpb.ID]struct{}
		droppedDescriptors   map[descpb.ID]struct{}
		admins               map[string]struct{}

		jobOwnerUser      string
		changefeedTargets []descpb.ID

		allowSameUserAccess bool

		canAccess bool
		userErr   error
	}{
		{
			name: "controljob-sufficient-for-non-admin-jobs",

			user: username.MakeSQLUsernameFromPreNormalizedString("user1"),
			roleOptions: map[roleoption.Option]struct{}{
				roleoption.CONTROLJOB: {},
			},
			admins:       map[string]struct{}{},
			jobOwnerUser: "user2",
			canAccess:    true,
		},
		{
			name: "controljob-insufficient-for-admin-jobs",
			user: username.MakeSQLUsernameFromPreNormalizedString("user1"),
			roleOptions: map[roleoption.Option]struct{}{
				roleoption.CONTROLJOB: {},
			},
			admins:       map[string]struct{}{"user2": {}},
			jobOwnerUser: "user2",
			canAccess:    false,
			userErr:      pgerror.New(pgcode.InsufficientPrivilege, "foo"),
		},
		{
			name:                "users-access-their-own-jobs",
			user:                username.MakeSQLUsernameFromPreNormalizedString("user1"),
			roleOptions:         map[roleoption.Option]struct{}{},
			admins:              map[string]struct{}{},
			jobOwnerUser:        "user1",
			allowSameUserAccess: true,
			canAccess:           true,
		},
		{
			name:         "users-cannot-access-their-own-jobs",
			user:         username.MakeSQLUsernameFromPreNormalizedString("user1"),
			roleOptions:  map[roleoption.Option]struct{}{},
			admins:       map[string]struct{}{},
			jobOwnerUser: "user1",
			canAccess:    false,
			userErr:      pgerror.New(pgcode.InsufficientPrivilege, "foo"),
		},
		{
			name:         "admins-see-admin-jobs",
			user:         username.MakeSQLUsernameFromPreNormalizedString("user1"),
			roleOptions:  map[roleoption.Option]struct{}{},
			admins:       map[string]struct{}{"user2": {}, "user1": {}},
			jobOwnerUser: "user2",
			canAccess:    true,
		},
		{
			name:        "changefeed-privilege-on-all-tables",
			user:        username.MakeSQLUsernameFromPreNormalizedString("user1"),
			roleOptions: map[roleoption.Option]struct{}{},
			admins:      map[string]struct{}{},

			changeFeedPrivileges: map[descpb.ID]struct{}{0: {}, 1: {}, 2: {}},
			changefeedTargets:    []descpb.ID{0, 1, 2},

			jobOwnerUser: "user2",
			canAccess:    true,
		},
		{
			name:        "changefeed-privilege-on-some-tables",
			user:        username.MakeSQLUsernameFromPreNormalizedString("user1"),
			roleOptions: map[roleoption.Option]struct{}{},
			admins:      map[string]struct{}{},

			changeFeedPrivileges: map[descpb.ID]struct{}{0: {}, 1: {}},
			changefeedTargets:    []descpb.ID{0, 1, 2},

			jobOwnerUser: "user2",
			canAccess:    false,
			userErr:      pgerror.New(pgcode.InsufficientPrivilege, "foo"),
		},
		{
			name:        "changefeed-priv-on-some-tables-with-dropped",
			user:        username.MakeSQLUsernameFromPreNormalizedString("user1"),
			roleOptions: map[roleoption.Option]struct{}{},
			admins:      map[string]struct{}{},

			changeFeedPrivileges: map[descpb.ID]struct{}{0: {}, 1: {}},
			changefeedTargets:    []descpb.ID{0, 1, 2},
			droppedDescriptors:   map[descpb.ID]struct{}{2: {}},

			jobOwnerUser: "user2",
			canAccess:    true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testAuth := &testAuthAccessor{
				user:                 tc.user,
				roleOptions:          tc.roleOptions,
				changeFeedPrivileges: tc.changeFeedPrivileges,
				droppedDescriptors:   tc.droppedDescriptors,
				admins:               tc.admins,
				rand:                 rng,
			}

			ctx := context.Background()
			cfSpecs := makeChangefeedSpecs(tc.changefeedTargets)
			payload := makePayload(tc.jobOwnerUser, cfSpecs)
			canAccess, userErr, err := jobsprivilege.JobTypeSpecificPrivilegeCheck(
				ctx, testAuth, 0, payload, tc.allowSameUserAccess)

			assert.Equal(t, nil, err)
			assert.Equal(t, tc.canAccess, canAccess)
			if !canAccess {
				assert.Equal(t, pgerror.GetPGCode(tc.userErr), pgerror.GetPGCode(userErr))
			}
		})
	}
}
