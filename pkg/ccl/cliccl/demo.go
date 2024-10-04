// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cliccl

import (
	gosql "database/sql"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/cli/democluster"
)

// enableEnterpriseForDemo enables enterprise features for 'cockroach demo'.
// It is not intended for use for non-demo servers.
func enableEnterpriseForDemo(db *gosql.DB, org string) (func(), error) {
	_, err := db.Exec(`SET CLUSTER SETTING cluster.organization = $1`, org)
	if err != nil {
		return nil, err
	}
	return utilccl.TestingEnableEnterprise(), nil
}

func init() {
	// Set the EnableEnterprise function within cockroach demo.
	// This separation is done to avoid using enterprise features in an OSS/BSL build.
	democluster.EnableEnterprise = enableEnterpriseForDemo
}
