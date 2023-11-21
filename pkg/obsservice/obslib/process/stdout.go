// Copyright 2023 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package process

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
)

// TODO: Delete this file once a proper Processor is created for Statement Insights.

type InsightsStdoutProcessor struct{}

func (t *InsightsStdoutProcessor) Process(
	_ context.Context, stmtInsight *obspb.StatementInsightsStatistics,
) error {
	fmt.Println(stmtInsight)
	return nil
}

var _ EventProcessor[*obspb.StatementInsightsStatistics] = (*InsightsStdoutProcessor)(nil)
