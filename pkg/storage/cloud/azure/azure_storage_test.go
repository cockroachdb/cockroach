// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package azure

import (
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud/cloudtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type azureConfig struct {
	account, key, bucket string
}

func (a azureConfig) filePath(f string) string {
	return fmt.Sprintf("azure://%s/%s?%s=%s&%s=%s",
		a.bucket, f,
		AzureAccountKeyParam, url.QueryEscape(a.key),
		AzureAccountNameParam, url.QueryEscape(a.account))
}

func getAzureConfig() (azureConfig, error) {
	cfg := azureConfig{
		account: os.Getenv("AZURE_ACCOUNT_NAME"),
		key:     os.Getenv("AZURE_ACCOUNT_KEY"),
		bucket:  os.Getenv("AZURE_CONTAINER"),
	}
	if cfg.account == "" || cfg.key == "" || cfg.bucket == "" {
		return azureConfig{}, errors.New("AZURE_ACCOUNT_NAME, AZURE_ACCOUNT_KEY, AZURE_CONTAINER must all be set")
	}
	return cfg, nil
}
func TestAzure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cfg, err := getAzureConfig()
	if err != nil {
		skip.IgnoreLint(t, "Test not configured for Azure")
		return
	}
	testSettings := cluster.MakeTestingClusterSettings()
	cloudtestutils.CheckExportStore(t, cfg.filePath("backup-test"),
		false, security.RootUserName(), nil, nil, testSettings)
	cloudtestutils.CheckListFiles(
		t, cfg.filePath("listing-test"), security.RootUserName(), nil, nil, testSettings,
	)
}

func TestAntagonisticAzureRead(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cfg, err := getAzureConfig()
	if err != nil {
		skip.IgnoreLint(t, "Test not configured for Azure")
		return
	}
	testSettings := cluster.MakeTestingClusterSettings()

	conf, err := cloud.ExternalStorageConfFromURI(
		cfg.filePath("antagonistic-read"), security.RootUserName())
	require.NoError(t, err)

	cloudtestutils.CheckAntagonisticRead(t, conf, testSettings)
}
