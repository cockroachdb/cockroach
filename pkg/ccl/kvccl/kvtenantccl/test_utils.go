// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvtenantccl

import "github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"

// InitConnectorFactory initializes the kvtenant.Factory to the connectorFactory
// in CCL. This is used by CCL tests that do not enable the enterprise license,
// since doing so will also initialize the connectorFactory.
func InitConnectorFactory() {
	kvtenant.Factory = connectorFactory{}
}
