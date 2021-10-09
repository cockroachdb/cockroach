// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigsqltranslator

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// DisabledSQLTranslator is an empty implementation of spanconfig.SQLTranslator
// for testing purposes.
type DisabledSQLTranslator struct{}

// DisabledSQLTranslator implements the spanconfig.SQLTranslator interface.
var _ spanconfig.SQLTranslator = &DisabledSQLTranslator{}

// NewDisabled returns a new disabled SQLTranslator.
func NewDisabled() *DisabledSQLTranslator {
	return &DisabledSQLTranslator{}
}

// Translate implements the spanconfig.SQLTranslator interface.
func (d *DisabledSQLTranslator) Translate(
	_ context.Context, _ descpb.IDs,
) ([]roachpb.SpanConfigEntry, error) {
	return nil, errors.New("DisabledSQLTranslator is not intended for use")
}

// FullTranslate implements the spanconfig.SQLTranslator interface.
func (d *DisabledSQLTranslator) FullTranslate(
	_ context.Context,
) ([]roachpb.SpanConfigEntry, hlc.Timestamp, error) {
	return nil, hlc.Timestamp{}, errors.New("DisabledSQLTranslator is not intended for use")
}
