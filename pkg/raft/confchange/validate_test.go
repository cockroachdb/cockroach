// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package confchange

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/raft/quorum"
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/stretchr/testify/require"
)

func TestValidateProp(t *testing.T) {
	configNormal := &quorum.Config{Voters: quorum.JointConfig{
		quorum.MajorityConfig{1: struct{}{}, 2: struct{}{}, 3: struct{}{}},
		quorum.MajorityConfig{},
	}}
	configJoint := &quorum.Config{Voters: quorum.JointConfig{
		quorum.MajorityConfig{1: struct{}{}, 2: struct{}{}, 3: struct{}{}},
		quorum.MajorityConfig{1: struct{}{}, 2: struct{}{}, 4: struct{}{}},
	}}

	changeNormal := pb.ConfChangeV2{Changes: []pb.ConfChangeSingle{
		{Type: pb.ConfChangeAddNode, NodeID: 5},
	}}
	changeLeaveJoint := pb.ConfChangeV2{Changes: []pb.ConfChangeSingle{}}

	tests := []struct {
		name   string
		ctx    ValidationContext
		cc     pb.ConfChangeV2
		expErr string
	}{
		{
			name: "valid",
			ctx: ValidationContext{
				CurConfig:        configNormal,
				Applied:          10,
				PendingConfIndex: 5,
			},
			cc: changeNormal,
		},
		{
			name: "valid, just applied conf change",
			ctx: ValidationContext{
				CurConfig:        configNormal,
				Applied:          10,
				PendingConfIndex: 10,
			},
			cc: changeNormal,
		},
		{
			name: "invalid, unapplied conf change",
			ctx: ValidationContext{
				CurConfig:        configNormal,
				Applied:          10,
				PendingConfIndex: 15,
			},
			cc:     changeNormal,
			expErr: "possible unapplied conf change at index 15 \\(applied to 10\\)",
		},
		{
			name: "invalid, already in joint state",
			ctx: ValidationContext{
				CurConfig:        configJoint,
				Applied:          10,
				PendingConfIndex: 5,
			},
			cc:     changeNormal,
			expErr: "must transition out of joint config first",
		},
		{
			name: "invalid, not in joint state",
			ctx: ValidationContext{
				CurConfig:        configNormal,
				Applied:          10,
				PendingConfIndex: 5,
			},
			cc:     changeLeaveJoint,
			expErr: "not in joint state; refusing empty conf change",
		},
		{
			name: "invalid, already in joint state, validation disabled",
			ctx: ValidationContext{
				CurConfig:                         configJoint,
				Applied:                           10,
				PendingConfIndex:                  5,
				DisableValidationAgainstCurConfig: true,
			},
			cc: changeNormal,
		},
		{
			name: "invalid, not in joint state, validation disabled",
			ctx: ValidationContext{
				CurConfig:                         configNormal,
				Applied:                           10,
				PendingConfIndex:                  5,
				DisableValidationAgainstCurConfig: true,
			},
			cc: changeLeaveJoint,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateProp(tt.ctx, tt.cc)
			if tt.expErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Regexp(t, tt.expErr, err)
			}
		})
	}
}
