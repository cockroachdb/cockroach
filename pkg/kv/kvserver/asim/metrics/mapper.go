// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metrics

type rateMapper struct {
	prev []StoreMetrics
}

func (smr *rateMapper) rate(sms []StoreMetrics) []StoreMetrics {
	ret := []StoreMetrics{}
	if len(smr.prev) == len(sms) {
		for i := range sms {
			ret = append(ret, rateStoreMetricFields(smr.prev[i], sms[i]))
		}
	}

	smr.prev = sms
	return ret
}

func rateStoreMetricFields(smA StoreMetrics, smB StoreMetrics) StoreMetrics {
	rated := deltaStoreMetric(smA, smB)
	duration := smB.Tick.Sub(smA.Tick).Seconds()
	rated.WriteKeys /= int64(duration)
	rated.WriteBytes /= int64(duration)
	rated.ReadBytes /= int64(duration)
	rated.ReadKeys /= int64(duration)
	rated.RebalanceSentBytes /= int64(duration)
	rated.RebalanceRcvdBytes /= int64(duration)

	return rated
}

func deltaStoreMetric(smA StoreMetrics, smB StoreMetrics) StoreMetrics {
	delta := smB
	delta.WriteBytes -= smA.WriteBytes
	delta.WriteKeys -= smA.WriteKeys
	delta.ReadBytes -= smA.ReadBytes
	delta.ReadKeys -= smA.ReadKeys
	delta.RebalanceSentBytes -= smA.RebalanceSentBytes
	delta.RebalanceRcvdBytes -= smA.RebalanceRcvdBytes
	return delta
}
