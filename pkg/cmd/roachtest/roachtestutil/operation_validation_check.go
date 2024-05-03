// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package roachtestutil

import (
	"context"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/errors"
)

func getQPSAvg(
	ctx context.Context, c cluster.Cluster, o operation.Operation, adminURL string, dur time.Duration,
) (float64, error) {
	endTime := time.Now()
	startTime := endTime.Add(-dur)
	response, err := GetMetricsWithSamplePeriod(ctx, c, o, adminURL, startTime, endTime, 1*time.Second, []TsQuery{
		{Name: "cr.node.sql.query.count", QueryType: Rate, Sources: []string{"1"}},
	})
	if err != nil {
		return 0, err
	}
	qpsNums := response.Results[0].Datapoints
	var avgQPS float64
	for _, dp := range qpsNums {
		avgQPS += dp.Value
	}
	avgQPS /= float64(len(qpsNums))
	return avgQPS, nil
}

// ValidateQPSRecovers confirms that SQL QPS recovers to within 5% of pre-operation
// QPS within 5 minutes of operation completion. It also validates that QPS stays
// within 20% of pre-operation QPS before operation cleanup.
func ValidateQPSRecovers(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) (midVal, postVal registry.ValidationFunc, err error) {
	adminUIAddrs, err := c.ExternalAdminUIAddr(ctx, o.L(), c.Nodes(1))
	if err != nil {
		return nil, nil, err
	}
	adminURL := adminUIAddrs[0]

	preOperationQPS, err := getQPSAvg(ctx, c, o, adminURL, 5*time.Second)
	if err != nil {
		return nil, nil, err
	}

	midValidationFunc := func(ctx context.Context, o operation.Operation, c cluster.Cluster) error {
		// Verify that mid-operation QPS is within 20% of pre-operation QPS.
		midOperationQPS, err := getQPSAvg(ctx, c, o, adminURL, 5*time.Second)
		if err != nil {
			return err
		}
		if math.Abs(midOperationQPS-preOperationQPS)/preOperationQPS > 0.2 {
			return errors.Errorf("validation failed: QPS after operation run %.2f is more than 20 pct greater than or less than pre-operation QPS %.2f", midOperationQPS, preOperationQPS)
		}
		return nil
	}

	postValidationFunc := func(ctx context.Context, o operation.Operation, c cluster.Cluster) error {
		// Verify that post-operation QPS is within 5% of pre-operation QPS, at any
		// point within 5 minutes of operation completion.
		ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()
		var postOperationQPS float64
		for {
			var err error
			if err := ctx.Err(); err != nil {
				break
			}
			postOperationQPS, err = getQPSAvg(ctx, c, o, adminURL, 5*time.Second)
			if err != nil {
				return err
			}
			if math.Abs(postOperationQPS-preOperationQPS)/preOperationQPS <= 0.05 {
				return nil
			}
			// Wait 5 seconds before measuring again.
			time.Sleep(5 * time.Second)
		}

		return errors.Errorf("validation failed: QPS after operation cleanup %.2f is more than 5 pct greater than or less than pre-operation QPS %.2f", postOperationQPS, preOperationQPS)
	}

	return midValidationFunc, postValidationFunc, nil
}
