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
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestPutAzure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	accountName := os.Getenv("AZURE_ACCOUNT_NAME")
	accountKey := os.Getenv("AZURE_ACCOUNT_KEY")
	if accountName == "" || accountKey == "" {
		t.Skip("AZURE_ACCOUNT_NAME and AZURE_ACCOUNT_KEY env vars must be set")
	}
	bucket := os.Getenv("AZURE_CONTAINER")
	if bucket == "" {
		t.Skip("AZURE_CONTAINER env var must be set")
	}

	testExportStore(t, fmt.Sprintf("azure://%s/%s?%s=%s&%s=%s",
		bucket, "backup-test",
		cloudimpl.AzureAccountNameParam, url.QueryEscape(accountName),
		cloudimpl.AzureAccountKeyParam, url.QueryEscape(accountKey),
	), false, security.RootUser, nil, nil)
	testListFiles(
		t,
		fmt.Sprintf("azure://%s/%s?%s=%s&%s=%s",
			bucket, "listing-test",
			cloudimpl.AzureAccountNameParam, url.QueryEscape(accountName),
			cloudimpl.AzureAccountKeyParam, url.QueryEscape(accountKey),
		),
		security.RootUser, nil, nil,
	)
}
