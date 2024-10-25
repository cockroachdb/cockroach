package metric

import "github.com/cockroachdb/cockroach/pkg/util/envutil"

const EnableCloudBillingAccountingEnvVar = "COCKROACH_ENABLE_CLOUD_BILLING_ACCOUNTING"

var EnableCloudBillingAccounting = envutil.EnvOrDefaultBool(EnableCloudBillingAccountingEnvVar, false)
