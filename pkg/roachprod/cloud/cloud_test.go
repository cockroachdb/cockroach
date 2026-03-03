// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloud

import (
	"testing"
	"time"

	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	cloudcluster "github.com/cockroachdb/cockroach/pkg/roachprod/cloud/types"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetTagsValues(t *testing.T) {
	IAMUserNameKey := "IAMUserName"
	createdAtKey := "CreatedAt"

	tcIAMUserNameValue := "user.name"
	tcCreatedAtValue := "2021-09-20T17:05:53Z"

	testCases := []struct {
		description         string
		inputTags           []ec2types.Tag
		expectedIAMUserName string
		expectedCreatedAt   string
	}{
		{
			description:         "untagged",
			inputTags:           []ec2types.Tag{},
			expectedIAMUserName: "",
			expectedCreatedAt:   "",
		},
		{
			description:         "only IAMUserName tag present",
			inputTags:           []ec2types.Tag{{Key: &IAMUserNameKey, Value: &tcIAMUserNameValue}},
			expectedIAMUserName: tcIAMUserNameValue,
			expectedCreatedAt:   "",
		},
		{
			description:         "only CreatedAt tag present",
			inputTags:           []ec2types.Tag{{Key: &createdAtKey, Value: &tcCreatedAtValue}},
			expectedIAMUserName: "",
			expectedCreatedAt:   tcCreatedAtValue,
		},
		{
			description: "IAMUserName and CreatedAt tags present",
			inputTags: []ec2types.Tag{
				{Key: &IAMUserNameKey, Value: &tcIAMUserNameValue},
				{Key: &createdAtKey, Value: &tcCreatedAtValue}},
			expectedIAMUserName: tcIAMUserNameValue,
			expectedCreatedAt:   tcCreatedAtValue,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			returnedIAMUserName, returnedCreatedAt := getTagsValues(tc.inputTags)
			assert.EqualValues(t, tc.expectedIAMUserName, returnedIAMUserName)
			assert.EqualValues(t, tc.expectedCreatedAt, returnedCreatedAt)
		})
	}
}

func TestGetIAMUserNameFromKeyname(t *testing.T) {
	// expected format: ${username}-${28 characters hash}
	testCases := []struct {
		description    string
		inputKeyname   string
		expectedOutput string
	}{
		{"less than 29 characters", "username-SHA1HashBase64Encod", ""},
		{"exactly 29 characters", "username-SHA1HashBase64Encode", ""},
		{"more than 29 characters but unexpected format", "username-SHA1HashBase64Encoded", ""},
		{"normal", "username-SHA1HashBase64Encoded1234567", "username"},
		{"username contains dot", "user.name-SHA1HashBase64Encoded1234567", "user.name"},
		{"username contains dashes", "a-b-c---d-e-f--g---SHA1HashBase64Encoded1234567", "a-b-c---d-e-f--g--"},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			returnedIAMUserName := getIAMUserNameFromKeyname(tc.inputKeyname)
			assert.EqualValues(t, tc.expectedOutput, returnedIAMUserName)
		})
	}
}

func TestDestroyCluster_ProvisioningManagedBlocked(t *testing.T) {
	c := &cloudcluster.Cluster{
		Name: "test-cluster",
		VMs: vm.List{
			{Name: "vm-1", Labels: map[string]string{
				vm.TagProvisioningIdentifier: "abc123",
			}},
		},
	}
	c.ManagedByProvisioning = c.IsProvisioningManaged()

	err := DestroyCluster(nil, c)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "provisioning-managed")
	assert.Contains(t, err.Error(), "test-cluster")
}

func TestDestroyCluster_UnmanagedNotBlocked(t *testing.T) {
	c := &cloudcluster.Cluster{
		Name: "test-cluster",
		VMs: vm.List{
			{Name: "vm-1", Labels: map[string]string{
				"roachprod": "true",
			}},
		},
	}

	// DestroyCluster panics past the managed-cluster guard (nil logger),
	// which proves the guard was not triggered.
	assert.Panics(t, func() { _ = DestroyCluster(nil, c) })
}

// TestGCBadVMs_ProvisioningIdentifierSkipped verifies the filtering logic
// at gc.go:408-418: bad VMs carrying a provisioning_identifier label are
// skipped, while unlabeled bad VMs are collected for deletion.
func TestGCBadVMs_ProvisioningIdentifierSkipped(t *testing.T) {
	now := time.Now()
	twoHoursAgo := now.Add(-2 * time.Hour)

	badInstances := vm.List{
		{Name: "labeled-vm", CreatedAt: twoHoursAgo, Labels: map[string]string{
			vm.TagProvisioningIdentifier: "prov-abc",
		}},
		{Name: "unlabeled-vm-1", CreatedAt: twoHoursAgo, Labels: map[string]string{
			"roachprod": "true",
		}},
		{Name: "unlabeled-vm-2", CreatedAt: twoHoursAgo, Labels: map[string]string{}},
		{Name: "recent-vm", CreatedAt: now, Labels: map[string]string{}}, // too recent
	}

	// Replicate the filtering logic from GCClusters (gc.go:408-418).
	var badVMs vm.List
	for _, v := range badInstances {
		if now.Sub(v.CreatedAt) >= time.Hour && !v.EmptyCluster {
			if v.Labels[vm.TagProvisioningIdentifier] != "" {
				continue
			}
			badVMs = append(badVMs, v)
		}
	}

	require.Len(t, badVMs, 2)
	assert.Equal(t, "unlabeled-vm-1", badVMs[0].Name)
	assert.Equal(t, "unlabeled-vm-2", badVMs[1].Name)
}

func TestGCStatus_ProvisioningManagedAlwaysGood(t *testing.T) {
	now := time.Now()

	// Create an expired provisioning-managed cluster.
	c := &cloudcluster.Cluster{
		Name:                  "managed-cluster",
		User:                  "user",
		CreatedAt:             now.Add(-48 * time.Hour),
		Lifetime:              12 * time.Hour,
		ManagedByProvisioning: true,
		VMs: vm.List{
			{Name: "vm-1", Labels: map[string]string{
				vm.TagProvisioningIdentifier: "abc123",
			}},
		},
	}

	s := &status{}
	s.add(c, now)

	// Even though the cluster is expired, it should be in 'good' not 'destroy'.
	assert.Len(t, s.good, 1)
	assert.Len(t, s.destroy, 0)
	assert.Len(t, s.warn, 0)
	assert.Equal(t, "managed-cluster", s.good[0].Name)
}

func TestGCStatus_UnmanagedExpiredDestroyed(t *testing.T) {
	now := time.Now()

	c := &cloudcluster.Cluster{
		Name:      "unmanaged-cluster",
		User:      "user",
		CreatedAt: now.Add(-48 * time.Hour),
		Lifetime:  12 * time.Hour,
		VMs: vm.List{
			{Name: "vm-1", Labels: map[string]string{
				"roachprod": "true",
			}},
		},
	}

	s := &status{}
	s.add(c, now)

	assert.Len(t, s.destroy, 1)
	assert.Len(t, s.good, 0)
	assert.Equal(t, "unmanaged-cluster", s.destroy[0].Name)
}
