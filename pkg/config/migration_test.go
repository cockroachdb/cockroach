// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Tristan Rice (rice@fn.lc)

package config

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/gogo/protobuf/proto"
)

func TestMigrateZoneConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		input, want proto.Message
	}{
		{
			&ZoneConfig{
				ReplicaAttrs: []roachpb.Attributes{
					{Attrs: []string{"foo"}},
					{},
					{},
				},
			},
			&ZoneConfig{
				NumReplicas: 3,
				Constraints: Constraints{
					Constraints: []Constraint{
						{
							Type:  Constraint_POSITIVE,
							Value: "foo",
						},
					},
				},
			},
		},
		{
			&ZoneConfig{
				NumReplicas: 3,
				Constraints: Constraints{
					Constraints: []Constraint{
						{
							Type:  Constraint_POSITIVE,
							Value: "foo",
						},
					},
				},
			},
			&ZoneConfig{
				NumReplicas: 3,
				Constraints: Constraints{
					Constraints: []Constraint{
						{
							Type:  Constraint_POSITIVE,
							Value: "foo",
						},
					},
				},
			},
		},
	}

	for i, tc := range testCases {
		var val roachpb.Value
		if err := val.SetProto(tc.input); err != nil {
			t.Fatal(err)
		}
		out, err := MigrateZoneConfig(&val)
		if err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(tc.want, &out) {
			t.Errorf("%d: MigrateZoneConfig(%+v) = %+v; not %+v", i, tc.input, out, tc.want)
		}
	}
}
