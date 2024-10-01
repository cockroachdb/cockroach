// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
