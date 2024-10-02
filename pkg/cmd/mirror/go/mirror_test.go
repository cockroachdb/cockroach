// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestModulePathToBazelRepoName(t *testing.T) {
	require.Equal(t, modulePathToBazelRepoName("github.com/alecthomas/template"), "com_github_alecthomas_template")
	require.Equal(t, modulePathToBazelRepoName("github.com/aws/aws-sdk-go-v2/service/iam"), "com_github_aws_aws_sdk_go_v2_service_iam")
	require.Equal(t, modulePathToBazelRepoName("github.com/Azure/go-ansiterm"), "com_github_azure_go_ansiterm")
	require.Equal(t, modulePathToBazelRepoName("gopkg.in/yaml.v3"), "in_gopkg_yaml_v3")
	require.Equal(t, modulePathToBazelRepoName("collectd.org"), "org_collectd")
}
