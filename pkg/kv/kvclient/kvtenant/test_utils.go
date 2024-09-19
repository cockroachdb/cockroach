// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvtenant

// InitTestConnectorFactory replaces the kv connector factory with a lenient factory that does no filtering
// on KV Addresses. This is useful for tests that do not want to be restricted to a loopback only address.
func InitTestConnectorFactory() {
	Factory = testConnectorFactory{}
}

// testConnectorFactory is a connector factory that does no filtering on KV Addresses.
type testConnectorFactory struct{}

// NewConnector creates a new connector with the given configuration.
func (testConnectorFactory) NewConnector(
	cfg ConnectorConfig, addressConfig KVAddressConfig,
) (Connector, error) {
	return NewConnector(cfg, CombineKVAddresses(addressConfig)), nil
}
