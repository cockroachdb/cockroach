package directory

import (
	"fmt"
	"strconv"
)

const (
	// SystemID is the ID of the internal, privileged system tenant.
	SystemID TenantID = 1
)

// TenantID is a unique ID associated with each tenant in a multi-tenant
// CockroachDB cluster.
//
// For more information, see:
//   https://github.com/cockroachdb/cockroach/blob/release-20.2/pkg/roachpb/tenant.go#L19-L24
type TenantID uint64

// IsSystemID returns true if this is the internal, privileged system tenant.
// The system tenant has administrative access and can create and destroy other
// tenants.
func (t TenantID) IsSystemID() bool {
	return t == SystemID
}

// Tag returns the tenant's "tag", which looks like this:
//
//   tenant-50
//
// It is commonly used as the name of a CrdbTenant object.
func (t TenantID) Tag() string {
	return fmt.Sprintf("tenant-%d", t)
}

func (t TenantID) String() string {
	return strconv.FormatUint(uint64(t), 10)
}
