package server

import (
	"net"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestUpdateLocalityAddresses_UpdatesPortCorrectly(t *testing.T) {
	// Given a slice of locality addresses with host parts and a valid advertised address.
	originalAddrs := []roachpb.LocalityAddress{
		{
			Address:      *util.NewUnresolvedAddr("tcp", "192.168.1.1:1111"),
			LocalityTier: roachpb.Tier{Key: "region", Value: "east"},
		},
		{
			Address:      *util.NewUnresolvedAddr("tcp", "10.0.0.2:2222"),
			LocalityTier: roachpb.Tier{Key: "region", Value: "west"},
		},
	}
	advertiseAddr := "127.0.0.1:8080"

	// When updateLocalityAddressesForSharedSecondaryTenants is called.
	updatedAddrs, err := updateLocalityAddressesForSharedSecondaryTenants(originalAddrs, advertiseAddr)

	// Then no error is returned and each address has its port replaced.
	require.NoError(t, err)
	_, newPort, err := net.SplitHostPort(advertiseAddr)
	require.NoError(t, err)
	for _, addr := range updatedAddrs {
		host, port, err := net.SplitHostPort(addr.Address.AddressField)
		require.NoError(t, err)
		require.Equal(t, newPort, port)
		require.NotEmpty(t, host)
	}
}

func TestUpdateLocalityAddresses_EmptyInputReturnsEmptySlice(t *testing.T) {
	// Given an empty slice of locality addresses and a valid advertised address.
	var originalAddrs []roachpb.LocalityAddress
	advertiseAddr := "127.0.0.1:8080"

	// When updateLocalityAddressesForSharedSecondaryTenants is called.
	updatedAddrs, err := updateLocalityAddressesForSharedSecondaryTenants(originalAddrs, advertiseAddr)

	// Then no error is returned and the result is an empty slice.
	require.NoError(t, err)
	require.Empty(t, updatedAddrs)
}

func TestUpdateLocalityAddresses_InvalidAdvertiseAddrReturnsError(t *testing.T) {
	// Given a valid slice of locality addresses and an invalid advertise address.
	originalAddrs := []roachpb.LocalityAddress{
		{
			Address:      *util.NewUnresolvedAddr("tcp", "192.168.1.1:1111"),
			LocalityTier: roachpb.Tier{Key: "region", Value: "east"},
		},
	}
	invalidAdvertiseAddr := "127.0.0.1" // missing port

	// When updateLocalityAddressesForSharedSecondaryTenants is called.
	_, err := updateLocalityAddressesForSharedSecondaryTenants(originalAddrs, invalidAdvertiseAddr)

	// Then an error is returned.
	require.Error(t, err)
}
