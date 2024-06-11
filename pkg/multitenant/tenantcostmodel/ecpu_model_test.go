// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantcostmodel

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEstimatedCPUModel(t *testing.T) {
	model := EstimatedCPUModel{
		ReadBatchCost:   1,
		ReadRequestCost: 2,
		ReadBytesCost: struct {
			PayloadSize []float64
			CPUPerByte  []EstimatedCPU
		}{
			PayloadSize: []float64{256, 1024, 4096},
			CPUPerByte:  []EstimatedCPU{1, 2, 3},
		},

		WriteBatchCost: struct {
			RatePerNode []float64
			CPUPerBatch []EstimatedCPU
		}{
			RatePerNode: []float64{100, 200, 400, 800},
			CPUPerBatch: []EstimatedCPU{0.5, 1, 1.5, 2},
		},
		WriteRequestCost: struct {
			BatchSize     []float64
			CPUPerRequest []EstimatedCPU
		}{
			BatchSize:     []float64{3, 6, 12, 25},
			CPUPerRequest: []EstimatedCPU{2, 3, 4, 5},
		},
		WriteBytesCost: struct {
			PayloadSize []float64
			CPUPerByte  []EstimatedCPU
		}{
			PayloadSize: []float64{256, 1024, 4096},
			CPUPerByte:  []EstimatedCPU{2, 4, 8},
		},
	}

	t.Run("roundtrip model through JSON", func(t *testing.T) {
		bytes, err := json.Marshal(model)
		require.NoError(t, err)

		var model2 EstimatedCPUModel
		require.NoError(t, json.Unmarshal(bytes, &model2))
		require.Equal(t, model, model2)
	})

	t.Run("metrics are smaller than min lookup values", func(t *testing.T) {
		reqInfo := RequestInfo{
			writeReplicas: 3,
			writeCount:    1,
			writeBytes:    1,
		}
		require.Equal(t, EstimatedCPU(13.5), model.RequestCost(reqInfo, 50))

		respInfo := ResponseInfo{
			isRead:    true,
			readCount: 10,
			readBytes: 100,
		}
		require.Equal(t, EstimatedCPU(121), model.ResponseCost(respInfo))
	})

	t.Run("metrics are equal to min lookup values", func(t *testing.T) {
		reqInfo := RequestInfo{
			writeReplicas: 3,
			writeCount:    3,
			writeBytes:    256,
		}
		require.Equal(t, EstimatedCPU(1555.5), model.RequestCost(reqInfo, 100))

		respInfo := ResponseInfo{
			isRead:    true,
			readCount: 10,
			readBytes: 256,
		}
		require.Equal(t, EstimatedCPU(277), model.ResponseCost(respInfo))
	})

	t.Run("metrics between lookup values", func(t *testing.T) {
		reqInfo := RequestInfo{
			writeReplicas: 5,
			writeCount:    9,
			writeBytes:    640,
		}
		require.Equal(t, EstimatedCPU(9761.25), model.RequestCost(reqInfo, 150))

		respInfo := ResponseInfo{
			isRead:    true,
			readCount: 50,
			readBytes: 2560,
		}
		require.Equal(t, EstimatedCPU(6501), model.ResponseCost(respInfo))
	})

	t.Run("metrics are equal to max lookup values", func(t *testing.T) {
		reqInfo := RequestInfo{
			writeReplicas: 3,
			writeCount:    25,
			writeBytes:    4096,
		}
		require.Equal(t, EstimatedCPU(98_685), model.RequestCost(reqInfo, 800))

		respInfo := ResponseInfo{
			isRead:    true,
			readCount: 100,
			readBytes: 4096,
		}
		require.Equal(t, EstimatedCPU(12_489), model.ResponseCost(respInfo))
	})

	t.Run("metrics are larger than max lookup values", func(t *testing.T) {
		reqInfo := RequestInfo{
			writeReplicas: 5,
			writeCount:    100,
			writeBytes:    10000,
		}
		require.Equal(t, EstimatedCPU(402_510), model.RequestCost(reqInfo, 1000))

		respInfo := ResponseInfo{
			isRead:    true,
			readCount: 1000,
			readBytes: 10000,
		}
		require.Equal(t, EstimatedCPU(32_001), model.ResponseCost(respInfo))
	})
}
