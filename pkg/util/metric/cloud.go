// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package metric

import "github.com/cockroachdb/cockroach/pkg/util/envutil"

const EnableCloudBillingAccountingEnvVar = "COCKROACH_ENABLE_CLOUD_BILLING_ACCOUNTING"

var EnableCloudBillingAccounting = envutil.EnvOrDefaultBool(EnableCloudBillingAccountingEnvVar, false)
