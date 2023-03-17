// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

// Package kvtenantccl provides utilities required by SQL-only tenant processes
// in order to interact with the key-value layer.
package kvtenantccl

import "github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"

func init() {
	kvtenant.Factory = connectorFactory{}
}

// connectorFactory implements kvtenant.ConnectorFactory.
type connectorFactory struct{}

// NewConnector creates a new kvtenant.Connector with the given configuration.
func (connectorFactory) NewConnector(
	cfg kvtenant.ConnectorConfig, addressConfig kvtenant.KVAddressConfig,
) (kvtenant.Connector, error) {
	return kvtenant.NewConnector(cfg, kvtenant.CombineKVAddresses(addressConfig)), nil
}
