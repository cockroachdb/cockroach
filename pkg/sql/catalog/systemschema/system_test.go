// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package systemschema

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
)

func TestSystemDatabaseSchemaBootstrapVersionFinalRelease(t *testing.T) {
	if clusterversion.Latest.IsFinal() {
		if SystemDatabaseSchemaBootstrapVersion != clusterversion.Latest.Version() {
			t.Fatalf("SystemDatabaseSchemaBootstrapVersion should be %s", clusterversion.Latest.Version())
		}
	}
}
