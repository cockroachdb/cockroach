// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package validate

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
)

const InvalidSchemaChangerStatePrefix = invalidSchemaChangerStatePrefix + ":"

func TestingSchemaChangerState(
	ctx context.Context, desc catalog.Descriptor,
) catalog.ValidationErrors {
	vea := validationErrorAccumulator{
		targetLevel:       catalog.ValidationLevelSelfOnly,
		activeVersion:     clusterversion.TestingClusterVersion,
		currentState:      validatingDescriptor,
		currentDescriptor: desc,
	}
	validateSchemaChangerState(desc, &vea)
	return vea.errors
}
