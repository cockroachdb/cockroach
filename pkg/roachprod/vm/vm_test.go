// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
		{2, 1, []int{0}},
		{2, 4, []int{0, 0, 1, 1}},
		{4, 2, []int{0, 1}},
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
			"leading and trailing hyphens", "--username-clustername-&", "username-clustername",
		},
		{
			"consecutive hyphens", "username---clustername", "username-clustername",
		},
		{
			"Unicode and other characters", "~/❦--u.ser_ऄn.a_meλ", "username",
		},
	}
	for _, c := range cases {
		t.Run(c.description, func(t *testing.T) {
			assert.EqualValues(t, c.expected, DNSSafeName(c.input))
		})
	}
}

func TestSanitizeLabel(t *testing.T) {
	cases := []struct{ label, expected string }{
		{"this/is/a/test", "this-is-a-test"},
		{"1234/abc!!", "1234-abc"},
		{"What/about-!!this one?", "what-about-this-one"},
		{"this-is-a-really/long-one-probably/over-63-characters/maybe?/let's_see", "this-is-a-really-long-one-probably-over-63-characters-maybe-let"},
	}

	for _, c := range cases {
		t.Run(c.expected, func(t *testing.T) {
			assert.EqualValues(t, c.expected, SanitizeLabel(c.label))
		})
	}
}

func TestParseArch(t *testing.T) {
	cases := []struct {
		arch     string
		expected CPUArch
	}{
		{"amd64", ArchAMD64},
		{"arm64", ArchARM64},
		{"Intel", ArchAMD64},
		{"x86_64", ArchAMD64},
		{"aarch64", ArchARM64},
		{"Intel Cascade Lake", ArchAMD64},
		{"Ampere Altra", ArchARM64},
		// E.g., GCE returns this when VM is still being provisioned.
		{"Unknown CPU Platform", ArchUnknown},
	}

	for _, c := range cases {
		t.Run(c.arch, func(t *testing.T) {
			assert.EqualValues(t, c.expected, ParseArch(c.arch))
		})
	}
}
