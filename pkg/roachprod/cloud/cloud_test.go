// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloud

import (
	"context"
	"errors"
	"io"
	"testing"

	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeDNSProvider struct {
	records     []vm.DNSRecord
	listErr     error
	deleteCalls int
	deleted     []string
}

type fakeProviderWithDNS struct {
	vm.Provider
	vm.DNSProvider
}

func (*fakeDNSProvider) CreateRecords(context.Context, ...vm.DNSRecord) error {
	return nil
}

func (*fakeDNSProvider) LookupSRVRecords(context.Context, string) ([]vm.DNSRecord, error) {
	return nil, nil
}

func (p *fakeDNSProvider) ListRecords(context.Context) ([]vm.DNSRecord, error) {
	return p.records, p.listErr
}

func (*fakeDNSProvider) DeleteSRVRecordsBySubdomain(context.Context, string) error {
	return nil
}

func (p *fakeDNSProvider) DeleteSRVRecordsByName(_ context.Context, names ...string) error {
	p.deleteCalls++
	p.deleted = append(p.deleted, names...)
	return nil
}

func (*fakeDNSProvider) DeletePublicRecordsByName(context.Context, ...string) error {
	return nil
}

func (*fakeDNSProvider) Domain() string {
	return "roachprod.example"
}

func (*fakeDNSProvider) PublicDomain() string {
	return "roachprod-public.example"
}

func (*fakeDNSProvider) ProviderName() string {
	return gce.ProviderName
}

func (*fakeDNSProvider) SyncDNS(*logger.Logger, vm.List) error {
	return nil
}

func (*fakeDNSProvider) SyncDNSWithContext(context.Context, *logger.Logger, vm.List) error {
	return nil
}

func installFakeGCEDNSProvider(t *testing.T, provider vm.DNSProvider) {
	t.Helper()
	oldProvider, hadProvider := vm.Providers[gce.ProviderName]
	vm.Providers[gce.ProviderName] = &fakeProviderWithDNS{DNSProvider: provider}
	t.Cleanup(func() {
		if hadProvider {
			vm.Providers[gce.ProviderName] = oldProvider
		} else {
			delete(vm.Providers, gce.ProviderName)
		}
	})
}

func testLogger(t *testing.T) *logger.Logger {
	t.Helper()
	cfg := logger.Config{Stdout: io.Discard, Stderr: io.Discard}
	l, err := cfg.NewLogger("")
	require.NoError(t, err)
	return l
}

func withoutSlack(t *testing.T) {
	t.Helper()
	oldToken := config.SlackToken
	config.SlackToken = ""
	t.Cleanup(func() {
		config.SlackToken = oldToken
	})
}

func TestGCClustersDoesNotRequireSlack(t *testing.T) {
	withoutSlack(t)
	require.NoError(t, GCClusters(testLogger(t), NewCloud(), false))
}

func TestGCDNSUsesConfiguredGCEProvider(t *testing.T) {
	withoutSlack(t)
	provider := &fakeDNSProvider{records: []vm.DNSRecord{
		{Name: "_sql._tcp.live.roachprod.example.", Type: vm.SRV},
		{Name: "_sql._tcp.dead.roachprod.example.", Type: vm.SRV},
	}}
	installFakeGCEDNSProvider(t, provider)

	cld := NewCloud()
	cld.Clusters["live"] = &Cluster{Name: "live"}
	require.NoError(t, GCDNS(testLogger(t), cld, false))
	require.Equal(t, 1, provider.deleteCalls)
	require.Equal(t, []string{"_sql._tcp.dead.roachprod.example."}, provider.deleted)
}

func TestGCDNSSkipsDeleteWhenNoRecordsAreDangling(t *testing.T) {
	withoutSlack(t)
	provider := &fakeDNSProvider{records: []vm.DNSRecord{
		{Name: "_sql._tcp.live.roachprod.example.", Type: vm.SRV},
	}}
	installFakeGCEDNSProvider(t, provider)

	cld := NewCloud()
	cld.Clusters["live"] = &Cluster{Name: "live"}
	require.NoError(t, GCDNS(testLogger(t), cld, false))
	require.Zero(t, provider.deleteCalls)
	require.Empty(t, provider.deleted)
}

func TestGCDNSReportsUnavailableProvider(t *testing.T) {
	oldProvider, hadProvider := vm.Providers[gce.ProviderName]
	delete(vm.Providers, gce.ProviderName)
	t.Cleanup(func() {
		if hadProvider {
			vm.Providers[gce.ProviderName] = oldProvider
		}
	})

	err := GCDNS(testLogger(t), NewCloud(), false)
	require.ErrorContains(t, err, "GCE DNS provider is unavailable")
	require.ErrorContains(t, err, "--gce-infra-project")
}

func TestGCDNSPropagatesListFailure(t *testing.T) {
	withoutSlack(t)
	provider := &fakeDNSProvider{listErr: errors.New("cannot access DNS zone")}
	installFakeGCEDNSProvider(t, provider)

	err := GCDNS(testLogger(t), NewCloud(), false)
	require.ErrorContains(t, err, "cannot access DNS zone")
	require.Zero(t, provider.deleteCalls)
	require.Empty(t, provider.deleted)
}

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
