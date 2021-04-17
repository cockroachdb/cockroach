// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenant

import (
	"fmt"
	"strconv"
)

const (
	// SystemID is the ID of the internal, privileged system tenant.
	SystemID ID = 1
)

// ID is a unique id associated with each tenant in a multi-tenant
// CockroachDB cluster.
//
// For more information, see:
//   https://github.com/cockroachdb/cockroach/blob/release-20.2/pkg/roachpb/tenant.go#L19-L24
type ID uint64

// IsSystemID returns true if this is the internal, privileged system tenant.
// The system tenant has administrative access and can create and destroy other
// tenants.
func (t ID) IsSystemID() bool {
	return t == SystemID
}

// Tag returns the tenant's "tag", which looks like this:
//
//   tenant-50
//
// It is commonly used as the name of a CrdbTenant object.
func (t ID) Tag() string {
	return fmt.Sprintf("tenant-%d", t)
}

func (t ID) String() string {
	return strconv.FormatUint(uint64(t), 10)
}
