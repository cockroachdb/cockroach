// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testsqlwatcher

import (
	"context"
	"errors"

	"github.com/cockroachdb/cockroach/pkg/spanconfig"
)

var _ spanconfig.SQLWatcher = &SQLWatcher{}

// SQLWatcher implements the spanconfig.SQLWatcher interface for testing
// purposes.
type SQLWatcher struct{}

func New() *SQLWatcher {
	return &SQLWatcher{}
}

func (*SQLWatcher) Watch(_ context.Context) (<-chan spanconfig.Update, error) {
	return nil, errors.New("test reconciler does not actually reconcile")
}
