// Copyright 2023 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package transform

import (
	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

type StmtInsightTransformer struct {
}

var _ EventTransformer[*obspb.StatementInsightsStatistics] = (*StmtInsightTransformer)(nil)

func (t *StmtInsightTransformer) Transform(
	event *obspb.Event,
) (*obspb.StatementInsightsStatistics, error) {
	var insight obspb.StatementInsightsStatistics
	if err := protoutil.Unmarshal(event.LogRecord.Body.GetBytesValue(), &insight); err != nil {
		return nil, err
	}
	return &insight, nil
}
