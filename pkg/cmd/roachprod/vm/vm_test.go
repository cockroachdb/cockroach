// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package vm

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestZonePlacement(t *testing.T) {
	for i, c := range []struct {
		numZones, numNodes int
		expected           []int
	}{
		{1, 1, []int{0}},
		{1, 2, []int{0, 0}},
		{2, 4, []int{0, 0, 1, 1}},
		{2, 5, []int{0, 0, 1, 1, 0}},
		{3, 11, []int{0, 0, 0, 1, 1, 1, 2, 2, 2, 0, 1}},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			assert.EqualValues(t, c.expected, ZonePlacement(c.numZones, c.numNodes))
		})
	}
}

func TestExpandZonesFlag(t *testing.T) {
	for i, c := range []struct {
		input, output []string
		expErr        string
	}{
		{
			input:  []string{"us-east1-b:3", "us-west2-c:2"},
			output: []string{"us-east1-b", "us-east1-b", "us-east1-b", "us-west2-c", "us-west2-c"},
		},
		{
			input:  []string{"us-east1-b:3", "us-west2-c"},
			output: []string{"us-east1-b", "us-east1-b", "us-east1-b", "us-west2-c"},
		},
		{
			input:  []string{"us-east1-b", "us-west2-c"},
			output: []string{"us-east1-b", "us-west2-c"},
		},
		{
			input:  []string{"us-east1-b", "us-west2-c:a2"},
			expErr: "failed to parse",
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			expanded, err := ExpandZonesFlag(c.input)
			if c.expErr != "" {
				if assert.Error(t, err) {
					assert.Regexp(t, c.expErr, err.Error())
				}
			} else {
				assert.EqualValues(t, c.output, expanded)
			}
		})
	}
}

func TestVM_ZoneEntry(t *testing.T) {
	cases := []struct {
		description string
		vm          VM
		expected    string
		expErr      string
	}{
		{
			description: "Normal length",
			vm:          VM{Name: "just_a_test", PublicIP: "1.1.1.1"},
			expected:    "just_a_test 60 IN A 1.1.1.1\n",
		},
		{
			description: "Too long name",
			vm: VM{
				Name:     "very_very_very_very_very_very_very_very_very_very_very_very_very_very_long_name",
				PublicIP: "1.1.1.1",
			},
			expErr: "Name too long",
		},
		{
			description: "Missing IP",
			vm:          VM{Name: "just_a_test"},
			expErr:      "Missing IP address",
		},
	}
	for _, c := range cases {
		t.Run(c.description, func(t *testing.T) {
			expanded, err := c.vm.ZoneEntry()
			if c.expErr != "" {
				if assert.Error(t, err) {
					assert.Regexp(t, c.expErr, err.Error())
				}
			} else {
				assert.EqualValues(t, c.expected, expanded)
			}
		})
	}
}

func TestDNSSafeAccount(t *testing.T) {

	cases := []struct {
		description, input, expected string
	}{
		{
			"regular", "username", "username",
		},
		{
			"mixed case", "UserName", "username",
		},
		{
			"dot", "user.name", "username",
		},
		{
			"underscore", "user_name", "username",
		},
		{
			"dot and underscore", "u.ser_n.a_me", "username",
		},
		{
			"Unicode and other characters", "~/❦u.ser_ऄn.a_meλ", "username",
		},
	}
	for _, c := range cases {
		t.Run(c.description, func(t *testing.T) {
			assert.EqualValues(t, DNSSafeAccount(c.input), c.expected)
		})
	}
}
