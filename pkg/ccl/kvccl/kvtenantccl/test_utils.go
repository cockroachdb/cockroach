// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvtenantccl

import "github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"

// InitConnectorFactory initializes the kvtenant.Factory to the connectorFactory
// in CCL. This is used by CCL tests that do not enable the enterprise license,
// since doing so will also initialize the connectorFactory.
func InitConnectorFactory() {
	kvtenant.Factory = connectorFactory{}
}
