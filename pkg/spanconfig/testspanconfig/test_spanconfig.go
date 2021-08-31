// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testspanconfig

import (
	"context"
	"errors"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

var _ spanconfig.SQLWatcher = &SQLWatcher{}

// SQLWatcher implements the spanconfig.SQLWatcher interface for testing
// purposes.
type SQLWatcher struct{}

// WatchForSQLUpdates implements the spanconfig.SQLWatcher interface.
func (*SQLWatcher) WatchForSQLUpdates(
	_ context.Context, _ hlc.Timestamp, _ spanconfig.SQLWatcherHandleFunc,
) error {
	return errors.New("test sql watcher is not intended for use")
}

// Close implements the spanconfig.SQLWatcher interface.
func (*SQLWatcher) Close() {
	return
}

var _ spanconfig.SQLTranslator = &SQLTranslator{}

// SQLTranslator implements the spanconfig.SQLTranslator interface for testing
// purposes.
type SQLTranslator struct{}

// Translate implements the spanconfig.SQLTranslator interface.
func (*SQLTranslator) Translate(
	_ context.Context, _ descpb.IDs,
) ([]roachpb.SpanConfigEntry, error) {
	return nil, errors.New("test sql translator is not intended for use")
}

// FullTranslate implements the spanconfig.SQLTranslator interface.
func (*SQLTranslator) FullTranslate(
	_ context.Context,
) ([]roachpb.SpanConfigEntry, hlc.Timestamp, error) {
	return nil, hlc.Timestamp{}, errors.New("test sql translator is not intended for use")
}
