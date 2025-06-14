// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storageconfig

// ProvisionedRate defines the provisioned rate for a store.
type ProvisionedRate struct {
	// ProvisionedBandwidth is the bandwidth provisioned for a store in bytes/s.
	ProvisionedBandwidth int64
	// In the future, we may add more fields here for IOPS or separate read and
	// write bandwidth.
}
