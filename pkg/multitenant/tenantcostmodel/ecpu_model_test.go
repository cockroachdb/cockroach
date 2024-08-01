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
		BackgroundCPU: struct {
			Amount       EstimatedCPU
			Amortization float64
		}{
			Amount:       0.5,
			Amortization: 5,
		},
	}

	t.Run("roundtrip model through JSON", func(t *testing.T) {
		bytes, err := json.Marshal(model)
		require.NoError(t, err)

		var model2 EstimatedCPUModel
		require.NoError(t, json.Unmarshal(bytes, &model2))
		require.Equal(t, model, model2)
	})

	t.Run("read/write counts are 0", func(t *testing.T) {
		reqInfo := RequestInfo{
			writeReplicas: 3,
			writeCount:    0,
			writeBytes:    1,
		}
		require.Equal(t, EstimatedCPU(0), model.RequestCost(reqInfo, 50))

		respInfo := ResponseInfo{
			isRead:    true,
			readCount: 0,
			readBytes: 100,
		}
		require.Equal(t, EstimatedCPU(101), model.ResponseCost(respInfo))
	})

	t.Run("read/write counts are 1", func(t *testing.T) {
		reqInfo := RequestInfo{
			writeReplicas: 3,
			writeCount:    1,
			writeBytes:    1,
		}
		require.Equal(t, EstimatedCPU(7.5), model.RequestCost(reqInfo, 50))

		respInfo := ResponseInfo{
			isRead:    true,
			readCount: 1,
			readBytes: 100,
		}
		require.Equal(t, EstimatedCPU(101), model.ResponseCost(respInfo))
	})

	t.Run("metrics are smaller than min lookup values", func(t *testing.T) {
		reqInfo := RequestInfo{
			writeReplicas: 3,
			writeCount:    2,
			writeBytes:    1,
		}
		require.Equal(t, EstimatedCPU(13.5), model.RequestCost(reqInfo, 50))

		respInfo := ResponseInfo{
			isRead:    true,
			readCount: 10,
			readBytes: 100,
		}
		require.Equal(t, EstimatedCPU(119), model.ResponseCost(respInfo))
	})

	t.Run("metrics are equal to min lookup values", func(t *testing.T) {
		reqInfo := RequestInfo{
			writeReplicas: 3,
			writeCount:    3,
			writeBytes:    256,
		}
		require.Equal(t, EstimatedCPU(1549.5), model.RequestCost(reqInfo, 100))

		respInfo := ResponseInfo{
			isRead:    true,
			readCount: 10,
			readBytes: 256,
		}
		require.Equal(t, EstimatedCPU(275), model.ResponseCost(respInfo))
	})

	t.Run("metrics between lookup values", func(t *testing.T) {
		reqInfo := RequestInfo{
			writeReplicas: 5,
			writeCount:    9,
			writeBytes:    640,
		}
		require.Equal(t, EstimatedCPU(9743.75), model.RequestCost(reqInfo, 150))

		respInfo := ResponseInfo{
			isRead:    true,
			readCount: 50,
			readBytes: 2560,
		}
		require.Equal(t, EstimatedCPU(6499), model.ResponseCost(respInfo))
	})

	t.Run("metrics are equal to max lookup values", func(t *testing.T) {
		reqInfo := RequestInfo{
			writeReplicas: 3,
			writeCount:    25,
			writeBytes:    4096,
		}
		require.Equal(t, EstimatedCPU(98_670), model.RequestCost(reqInfo, 800))

		respInfo := ResponseInfo{
			isRead:    true,
			readCount: 100,
			readBytes: 4096,
		}
		require.Equal(t, EstimatedCPU(12_487), model.ResponseCost(respInfo))
	})

	t.Run("metrics are larger than max lookup values", func(t *testing.T) {
		reqInfo := RequestInfo{
			writeReplicas: 5,
			writeCount:    100,
			writeBytes:    10000,
		}
		require.Equal(t, EstimatedCPU(402_485), model.RequestCost(reqInfo, 1000))

		respInfo := ResponseInfo{
			isRead:    true,
			readCount: 1000,
			readBytes: 10000,
		}
		require.Equal(t, EstimatedCPU(31_999), model.ResponseCost(respInfo))
	})
}
