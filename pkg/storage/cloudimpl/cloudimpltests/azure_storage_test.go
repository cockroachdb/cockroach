// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloudimpltests

import (
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
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
		cloudimpl.AzureAccountKeyParam, url.QueryEscape(a.key),
		cloudimpl.AzureAccountNameParam, url.QueryEscape(a.account))
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
func TestPutAzure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cfg, err := getAzureConfig()
	if err != nil {
		skip.IgnoreLint(t, "Test not configured for Azure")
		return
	}

	testExportStore(t, cfg.filePath("backup-test"),
		false, security.RootUserName(), nil, nil)
	testListFiles(
		t, cfg.filePath("listing-test"), security.RootUserName(), nil, nil,
	)
}

func TestAntagonisticAzureRead(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cfg, err := getAzureConfig()
	if err != nil {
		skip.IgnoreLint(t, "Test not configured for Azure")
		return
	}

	conf, err := cloudimpl.ExternalStorageConfFromURI(
		cfg.filePath("antagonistic-read"), security.RootUserName())
	require.NoError(t, err)

	testAntagonisticRead(t, conf)
}
