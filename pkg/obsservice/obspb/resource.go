// Copyright 2022 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package obspb

// Names of fields used for Resource.Attributes.
const (
	ClusterID string = "ClusterID"
	// NodeID corresponds to either a roachpb.NodeID (for KV nodes) or a
	// base.SQLInstanceID (for SQL tenants).
	NodeID            string = "NodeID"
	NodeBinaryVersion string = "NodeBinaryVersion"
)
