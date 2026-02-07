// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package keys

import "github.com/cockroachdb/cockroach/pkg/clusterversion"

func init() {
	// PublicSchemaID refers to old references where Public schemas are
	// descriptorless.
	// TODO(richardjcai): This should be fully removed in 22.2.
	clusterversion.Refer(clusterversion.V22_1, PublicSchemaID)
}
