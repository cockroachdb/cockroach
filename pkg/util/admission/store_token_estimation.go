// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package admission

import "github.com/cockroachdb/pebble"

// The logic in this file deals with token estimation for a store write in two
// situations: (a) at admission time, (b) when the admitted work is done. At
// (a) we have no information provided about the work size (NB: this choice is
// debatable, since for ingests we could extract some information pre-request
// evaluation). At (b) we have the information in StoreWorkDoneInfo, which
// gets aggregated by the StoreWorkQueue into storeAdmissionStats.
//
// Both kinds of token estimation are guessing the tokens that a request
// should rightfully consume, based on models that are "trained" on actual
// resource consumption observed, and reported work sizes, in the past.
//
// The token estimation is complicated by the fact that many writes do not
// integrate with admission control. Specifically, even if they did not ask to
// be admitted, it would be beneficial for them to provide the information at
// (b), so we could subtract tokens for the work they did. The list of
// significant non-integrated writes is:
//
// 1. Range snapshot application (does ingestion).
//
// 2. State machine application for regular writes: StoreWorkDoneInfo,
//    WriteBytes only accounts for the write to the raft log.
//
// 3. Follower writes, to the raft log and to the state machine, both regular
//    and ingests.
//
// Over time, we should change the code to perform step (b) for (1) and for
// the raft log for (3). This will require noticing under raft that a write is
// at the leaseholder, so that we don't account for the raft log write a
// second time (since it has already been accounted for at proposal
// evaluation). The current code here is designed to fit that future world,
// while also attempting to deal with the limitations of this lack of
// integration.
//
// Specifically, in such a future world, even though we have the raft log
// writes for regular writes, and ingest bytes for ingestion requests, we have
// a mix of workloads that are concurrently doing writes. These could be
// multiple SQL-originating workloads with different size writes, and bulk
// workloads that are doing index backfills. We desire workload agnostic token
// estimation so a mix of various workloads can share the same token
// estimation model.
//
// We also have the immediate practical requirement that if there are bytes
// added to L0 (based on Pebble stats) that are unaccounted for by
// StoreWorkDoneInfo, we should compensate for that by adjusting our
// estimates. This may lead to per-work over-estimation, but that is better
// than an unhealthy LSM. A consequence of this is that we are willing to add
// tokens based on unaccounted write bytes to work that only did ingestion and
// willing to add tokens based on unaccounted ingested bytes to work that only
// did regular writes.
//
// We observe:
// - The lack of integration of state machine application can be mostly
//   handled by a multiplier on the bytes written to the raft log. Say a
//   multiplier of ~2.
//
// - We expect that most range snapshot applications will ingest into levels
//   below L0, so if we limit our attention to Pebble stats relating to
//   ingestion into L0, we may not see the effect of these unaccounted bytes,
//   which will result in more accurate estimation.
//
// - Ingests have some fraction that get ingested into L0, i.e., a multiplier
//   <= 1.
//
// Based on these observations we adopt a linear model for estimating the
// actual bytes (y), given the accounted bytes (x): y = a.x + b. The
// estimation of a and b is done by tokensLinearModelFitter. We constrain the
// interval of a to be [min,max] to prevent wild fluctuations and to account
// for what we know about the system:
// - For writes, we expect the multiplier a to be close to 2, due to the
//   subsequent application to the state machine. So it would be reasonable to
//   constrain a to [1, 2]. However, in experiments we've seen inconsistencies
//   between Pebble stats and admission control stats, due to choppiness in
//   work getting done, which is better modeled by allowing multiplier a to be
//   less constrained. So we use [0.5, 3].
//
// - For ingests, we expect the multiplier a to be <= 1, since some fraction
//   of the ingest goes into L0. So it would be reasonable to constrain a to
//   [0, 1]. For the same reason as the previous bullet, we use [0.001, 1.5].
//   This lower-bound of 0.001 is debatable, since it will cause some token
//   consumption even if all ingested bytes are going to levels below L0.
//   TODO(sumeer): consider lowering the lower bound, after experimentation.
//
// In both the writes and ingests case, y is the bytes being added to L0.
//
// NB: these linear models will be workload agnostic if most of the bytes are
// modeled via the a.x term, and not via the b term, since workloads are
// likely (at least for regular writes) to vary significantly in x.

// tokensLinearModel represents a model y = multiplier.x + constant.
type tokensLinearModel struct {
	multiplier float64
	// constant >= 0
	constant int64
}

// tokensLinearModelFitter fits y = multiplier.x + constant, based on the
// current interval and then exponentially smooths the multiplier and
// constant.
//
// This fitter is probably poor and could be improved by taking history into
// account in a cleverer way, such as looking at many past samples and doing
// linear regression, under the assumption that the workload is stable.
// However, the simple approach here should be an improvement on the additive
// approach we previously used.
//
//
// TODO(sumeer): improve the model based on realistic combinations of
// workloads (e.g. foreground writes + index backfills).
type tokensLinearModelFitter struct {
	// [multiplierMin, multiplierMax] constrains the multiplier.
	multiplierMin float64
	multiplierMax float64

	intLinearModel                tokensLinearModel
	smoothedLinearModel           tokensLinearModel
	smoothedPerWorkAccountedBytes int64

	// Should be set to true for the ingested bytes model: if all bytes are
	// ingested below L0, the actual bytes will be zero and the accounted bytes
	// non-zero. We need to update the model in this case.
	updateWithZeroActualNonZeroAccountedForIngestedModel bool
}

func makeTokensLinearModelFitter(
	multMin float64, multMax float64, updateWithZeroActualNonZeroAccountedForIngestedModel bool,
) tokensLinearModelFitter {
	return tokensLinearModelFitter{
		multiplierMin: multMin,
		multiplierMax: multMax,
		smoothedLinearModel: tokensLinearModel{
			multiplier: (multMin + multMax) / 2,
			constant:   1,
		},
		smoothedPerWorkAccountedBytes:                        1,
		updateWithZeroActualNonZeroAccountedForIngestedModel: updateWithZeroActualNonZeroAccountedForIngestedModel,
	}
}

// updateModelUsingIntervalStats updates the model, based on various stats
// over the last interval: the number of work items admitted (workCount), the
// bytes claimed by these work items (accountedBytes), and the actual bytes
// observed in the LSM for that interval (actualBytes).
//

// As mentioned earlier, the current fitting algorithm is probably poor, though an
// improvement on what we had previously. The approach taken is:
//
// - Fit the best model we can for the interval,
//   multiplier*accountedBytes + workCount*constant = actualBytes, while
//   minimizing the constant. We prefer the model to use the multiplier for
//   most of what it needs to account for actualBytes.
//   This exact model ignores inaccuracies due to integer arithmetic -- we
//   don't care about rounding errors since an error of 2 bytes per request is
//   inconsequential.
//
// - The multiplier has to conform to the [min,max] configured for this model,
//   and constant has to conform to a value >= 1. The constant is constrained
//   to be >=1 on the intuition that we want a request to consume at least 1
//   token -- it isn't clear that this intuition is meaningful in any way.
//
// - Exponentially smooth this exact model's multiplier and constant based on
//   history.
func (f *tokensLinearModelFitter) updateModelUsingIntervalStats(
	accountedBytes int64, actualBytes int64, workCount int64,
) {
	if workCount <= 1 || (actualBytes <= 0 &&
		(!f.updateWithZeroActualNonZeroAccountedForIngestedModel || accountedBytes <= 0)) {
		// Don't want to update the model if workCount is very low or actual bytes
		// is zero (except for the exceptions in the if-condition above).
		//
		// Not updating the model at all does have the risk that a large constant
		// will keep penalizing in the future. For example, if there are only
		// ingests, and the regular writes model had a large constant, it will
		// keep penalizing ingests. So we scale down the constant as if the new
		// model had a 0 value for the constant and the exponential smoothing
		// alpha was 0.5, i.e., halve the constant.
		f.intLinearModel = tokensLinearModel{}
		f.smoothedLinearModel.constant = max(1, f.smoothedLinearModel.constant/2)
		return
	}
	if actualBytes < 0 {
		actualBytes = 0
	}
	const alpha = 0.5
	if accountedBytes <= 0 {
		if actualBytes > 0 {
			// Anomaly. Assume that we will see smoothedPerWorkAccountedBytes in the
			// future. This prevents us from blowing up the constant in the model due
			// to this anomaly.
			accountedBytes = workCount * max(1, f.smoothedPerWorkAccountedBytes)
		} else {
			// actualBytes is also 0.
			accountedBytes = 1
		}
	} else {
		perWorkAccountedBytes := accountedBytes / workCount
		f.smoothedPerWorkAccountedBytes = int64(
			alpha*float64(perWorkAccountedBytes) + (1-alpha)*float64(f.smoothedPerWorkAccountedBytes))
	}
	// INVARIANT: workCount > 0, accountedBytes > 0, actualBytes >= 0.

	// Start with the lower bound of 1 on constant, since we want most of bytes
	// to be fitted using the multiplier. So workCount tokens go into that.
	constant := int64(1)
	// Then compute the multiplier.
	multiplier := float64(max(0, actualBytes-workCount*constant)) / float64(accountedBytes)
	// The multiplier may be too high or too low, so make it conform to
	// [min,max].
	if multiplier > f.multiplierMax {
		multiplier = f.multiplierMax
	} else if multiplier < f.multiplierMin {
		multiplier = f.multiplierMin
	}
	// This is the model with the multiplier as small or large as possible,
	// while minimizing constant (which is 1).
	modelBytes := int64(multiplier*float64(accountedBytes)) + (constant * workCount)
	// If the model is not accounting for all of actualBytes, we are forced to
	// increase the constant to cover the difference.
	if modelBytes < actualBytes {
		constantAdjust := (actualBytes - modelBytes) / workCount
		// Avoid overflow in case of bad stats.
		if constantAdjust+constant > 0 {
			constant += constantAdjust
		}
	}
	// The best model we can come up for the interval.
	f.intLinearModel = tokensLinearModel{
		multiplier: multiplier,
		constant:   constant,
	}
	// Smooth the multiplier and constant factors.
	f.smoothedLinearModel.multiplier = alpha*multiplier + (1-alpha)*f.smoothedLinearModel.multiplier
	f.smoothedLinearModel.constant = int64(
		alpha*float64(constant) + (1-alpha)*float64(f.smoothedLinearModel.constant))
}

type storePerWorkTokenEstimator struct {
	atAdmissionWorkTokens         int64
	atDoneWriteTokensLinearModel  tokensLinearModelFitter
	atDoneIngestTokensLinearModel tokensLinearModelFitter

	cumStoreAdmissionStats storeAdmissionStats
	cumL0WriteBytes        uint64
	cumL0IngestedBytes     uint64

	// Tracked for logging and copied out of here.
	aux perWorkTokensAux
}

// perWorkTokensAux encapsulates auxiliary (informative) numerical state that
// helps in understanding the behavior of storePerWorkTokenEstimator.
type perWorkTokensAux struct {
	intWorkCount                int64
	intL0WriteBytes             int64
	intL0IngestedBytes          int64
	intL0WriteAccountedBytes    int64
	intL0IngestedAccountedBytes int64
	intWriteLinearModel         tokensLinearModel
	intIngestedLinearModel      tokensLinearModel
}

func makeStorePerWorkTokenEstimator() storePerWorkTokenEstimator {
	return storePerWorkTokenEstimator{
		atAdmissionWorkTokens:         1,
		atDoneWriteTokensLinearModel:  makeTokensLinearModelFitter(0.5, 3, false),
		atDoneIngestTokensLinearModel: makeTokensLinearModelFitter(0.001, 1.5, true),
	}
}

// NB: first call to updateEstimates only initializes the cumulative values.
func (e *storePerWorkTokenEstimator) updateEstimates(
	l0Metrics pebble.LevelMetrics, admissionStats storeAdmissionStats,
) {
	if e.cumL0WriteBytes == 0 {
		e.cumStoreAdmissionStats = admissionStats
		e.cumL0WriteBytes = l0Metrics.BytesFlushed
		e.cumL0IngestedBytes = l0Metrics.BytesIngested
		return
	}
	intL0WriteBytes := int64(l0Metrics.BytesFlushed) - int64(e.cumL0WriteBytes)
	intL0IngestedBytes := int64(l0Metrics.BytesIngested) - int64(e.cumL0IngestedBytes)
	intWorkCount := int64(admissionStats.admittedCount) -
		int64(e.cumStoreAdmissionStats.admittedCount)
	intL0WriteAccountedBytes :=
		int64(admissionStats.writeAccountedBytes) - int64(e.cumStoreAdmissionStats.writeAccountedBytes)
	// Note that these are not really L0 ingested bytes, since we don't know how
	// many did go to L0.
	intL0IngestedAccountedBytes := int64(admissionStats.ingestedAccountedBytes) -
		int64(e.cumStoreAdmissionStats.ingestedAccountedBytes)
	e.atDoneWriteTokensLinearModel.updateModelUsingIntervalStats(
		intL0WriteAccountedBytes, intL0WriteBytes, intWorkCount)
	e.atDoneIngestTokensLinearModel.updateModelUsingIntervalStats(
		intL0IngestedAccountedBytes, intL0IngestedBytes, intWorkCount)

	intL0TotalBytes := intL0WriteBytes + intL0IngestedBytes
	if intWorkCount > 1 && intL0TotalBytes > 0 {
		// Update the atAdmissionWorkTokens
		intAtAdmissionWorkTokens := intL0TotalBytes / intWorkCount
		const alpha = 0.5
		e.atAdmissionWorkTokens = int64(alpha*float64(intAtAdmissionWorkTokens) +
			(1-alpha)*float64(e.atAdmissionWorkTokens))
		e.atAdmissionWorkTokens = max(1, e.atAdmissionWorkTokens)
	}
	e.cumStoreAdmissionStats = admissionStats
	e.cumL0WriteBytes = l0Metrics.BytesFlushed
	e.cumL0IngestedBytes = l0Metrics.BytesIngested
	e.aux = perWorkTokensAux{
		intWorkCount:                intWorkCount,
		intL0WriteBytes:             intL0WriteBytes,
		intL0IngestedBytes:          intL0IngestedBytes,
		intL0WriteAccountedBytes:    intL0WriteAccountedBytes,
		intL0IngestedAccountedBytes: intL0IngestedAccountedBytes,
		intWriteLinearModel:         e.atDoneWriteTokensLinearModel.intLinearModel,
		intIngestedLinearModel:      e.atDoneIngestTokensLinearModel.intLinearModel,
	}
}

func (e *storePerWorkTokenEstimator) getStoreRequestEstimatesAtAdmission() storeRequestEstimates {
	return storeRequestEstimates{writeTokens: e.atAdmissionWorkTokens}
}

func (e *storePerWorkTokenEstimator) getModelsAtAdmittedDone() (
	writeLM tokensLinearModel,
	ingestedLM tokensLinearModel,
) {
	return e.atDoneWriteTokensLinearModel.smoothedLinearModel,
		e.atDoneIngestTokensLinearModel.smoothedLinearModel
}

// TODO(sumeer):
// - Make followers tell admission control about the size of their write and
//   sstable bytes, without asking for permission. We will include these in
//   the admitted count and fit them to the same model, so that it is more
//   accurate. Additionally, we will scale down the bytes for
//   at-admission-tokens by the fraction
//   bytes-that-sought-permission/(bytes-that-sought-permission +
//   bytes-that-did-not-seek-permission)
//   Additionally, this work that did not seek permission will consume tokens.
//
// - Integrate snapshot ingests: these are likely to usually land in L6, so
//   may not fit the existing ingest model well. Additionally, we do not want
//   large range snapshots to consume a huge number of tokens. We do know how
//   many bytes were ingested into L0 for such snapshots. We can use those
//   bytes to hide the L0 increase caused by these snapshots so that they do
//   not affect the model. And not have them consume any tokens (this is a
//   simpler version of https://github.com/cockroachdb/cockroach/pull/80914,
//   and not the final solution).
