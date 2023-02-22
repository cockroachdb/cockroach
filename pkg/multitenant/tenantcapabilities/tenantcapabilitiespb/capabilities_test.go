package tenantcapabilitiespb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFlagFields(t *testing.T) {
	capabilities := TenantCapabilities{}
	for _, capabilityName := range TenantCapabilityNames {
		require.False(t, capabilities.GetFlagCapability(capabilityName))
	}
	for _, capabilityName := range TenantCapabilityNames {
		capabilities.SetFlagCapability(capabilityName, true)
	}
	require.Equal(t, TenantCapabilities{
		CanAdminSplit:      true,
		CanViewNodeInfo:    true,
		CanViewTSDBMetrics: true,
	}, capabilities)
}
