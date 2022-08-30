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
// We use models in which the "actual tokens" are computed as a linear
// function of the bytes claimed in StoreWorkDoneInfo (which we call
// accounted_bytes), i.e. actual_tokens = a*accounted_bytes + b, and the result
// of this computation (which can only be done after completion of the work)
// is used to acquire (without blocking) additional tokens. The model thus
// tries to make sure that one token reflects one byte of work. For example,
// if the model is initially (a=1, b=0) and each incoming request acquires
// 1000 tokens but ends up writing 2000 bytes, the model should update to
// roughly (a=2, b=0), and future requests will, upon completion, acquire an
// additional 1000 tokens to even the score. The actual fitting is performed
// on aggregates over recent requests, and the more work is done "outside" of
// admission control, the less useful the multiplier becomes; the model will
// degrade into one with a larger constant term and is expected to perform
// poorly.
//
// We now justify the use of a linear model. A model with only a constant term
// (the b term above) is not able to handle multiple simultaneous workloads
// executing on a node, since they can have very different work sizes. We
// desire workload agnostic token estimation so a mix of various workloads can
// share the same token estimation model. A model with
// actual_tokens=accounted_bytes is also not suitable for 2 reasons:
// - For writes (that are accomplished via the memtable) we only have the size
//   of the raft log entry in accounted_bytes and not the size of the later
//   state machine application.
//
// - For ingests, we also need to fit a model where accounted_bytes is the
//   size of the ingest, and actual_tokens is the size that landed in L0.
//
// We note that a multiplier term (the a term above) can accomplish both goals.
// The first bullet can be handled by a multiplier that is approximately 2.
// Ingests have some fraction that get ingested into L0, i.e., a multiplier
// <= 1.
//
// One complication with ingests is range snapshot application. They happen
// infrequently and can add a very large number of bytes, which are often
// ingested below L0. We don't want to skew our ingest models based on these
// range snapshots, so we explicitly ignore them in modeling.
//
// So now that we've justified the a term, one question arises is whether we
// need a b term. Historically we have had sources of error that are due to
// lack of integration with admission control, and we do not want to skew the
// a term significantly. So the fitting approach has a b term, but attempts to
// minimize the b term while keeping the a term within some configured bounds.
// The [min,max] bounds on the a term prevent wild fluctuations and are set
// based on what we know about the system.
//
// The estimation of a and b is done by tokensLinearModelFitter. It is used
// to fit 3 models.
// - [l0WriteLM] Mapping the write accounted bytes to bytes added to L0: We
//   expect the multiplier a to be close to 2, due to the subsequent
//   application to the state machine. So it would be reasonable to constrain
//   a to [1, 2]. However, in experiments we've seen inconsistencies between
//   Pebble stats and admission control stats, due to choppiness in work
//   getting done, which is better modeled by allowing multiplier a to be less
//   constrained. So we use [0.5, 3].
//
// - [l0IngestLM] Mapping the ingest accounted bytes (which is the total bytes
//   in the ingest, and not just to L0), to the bytes added to L0: We expect
//   the multiplier a to be <= 1, since some fraction of the ingest goes into
//   L0. So it would be reasonable to constrain a to [0, 1]. For the same
//   reason as the previous bullet, we use [0.001, 1.5]. This lower-bound of
//   0.001 is debatable, since it will cause some token consumption even if
//   all ingested bytes are going to levels below L0.
//   TODO(sumeer): consider lowering the lower bound, after experimentation.
//
// - [ingestLM] Mapping the ingest accounted bytes to the total ingested bytes
//   added to the LSM. We can expect a multiplier of 1. For now, we use bounds
//   of [0.5, 1.5].
//
// NB: these linear models will be workload agnostic if most of the bytes are
// modeled via the a.x term, and not via the b term, since workloads are
// likely (at least for regular writes) to vary significantly in x.

// See the comment above for the justification of these constants.
const l0WriteMultiplierMin = 0.5
const l0WriteMultiplierMax = 3.0
const l0IngestMultiplierMin = 0.001
const l0IngestMultiplierMax = 1.5
const ingestMultiplierMin = 0.5
const ingestMultiplierMax = 1.5

type storePerWorkTokenEstimator struct {
	atAdmissionWorkTokens           int64
	atDoneL0WriteTokensLinearModel  tokensLinearModelFitter
	atDoneL0IngestTokensLinearModel tokensLinearModelFitter
	// Unlike the models above that model bytes into L0, this model computes all
	// ingested bytes into the LSM.
	atDoneIngestTokensLinearModel tokensLinearModelFitter

	cumStoreAdmissionStats storeAdmissionStats
	cumL0WriteBytes        uint64
	cumL0IngestedBytes     uint64
	cumLSMIngestedBytes    uint64

	// Tracked for logging and copied out of here.
	aux perWorkTokensAux
}

// perWorkTokensAux encapsulates auxiliary (informative) numerical state that
// helps in understanding the behavior of storePerWorkTokenEstimator.
type perWorkTokensAux struct {
	intWorkCount              int64
	intL0WriteBytes           int64
	intL0IngestedBytes        int64
	intLSMIngestedBytes       int64
	intL0WriteAccountedBytes  int64
	intIngestedAccountedBytes int64
	intL0WriteLinearModel     tokensLinearModel
	intL0IngestedLinearModel  tokensLinearModel
	intIngestedLinearModel    tokensLinearModel

	// The bypassed count and bytes are also included in the overall interval
	// stats.
	intBypassedWorkCount              int64
	intL0WriteBypassedAccountedBytes  int64
	intIngestedBypassedAccountedBytes int64

	// The ignored bytes are included in intL0IngestedBytes, and in
	// intLSMWriteAndIngestedBytes, and may even be higher than that value
	// because these are from a different source.
	intL0IgnoredIngestedBytes int64
}

func makeStorePerWorkTokenEstimator() storePerWorkTokenEstimator {
	return storePerWorkTokenEstimator{
		atAdmissionWorkTokens: 1,
		atDoneL0WriteTokensLinearModel: makeTokensLinearModelFitter(
			l0WriteMultiplierMin, l0WriteMultiplierMax, false),
		atDoneL0IngestTokensLinearModel: makeTokensLinearModelFitter(
			l0IngestMultiplierMin, l0IngestMultiplierMax, true),
		atDoneIngestTokensLinearModel: makeTokensLinearModelFitter(
			ingestMultiplierMin, ingestMultiplierMax, false),
	}
}

// NB: first call to updateEstimates only initializes the cumulative values.
func (e *storePerWorkTokenEstimator) updateEstimates(
	l0Metrics pebble.LevelMetrics, cumLSMIngestedBytes uint64, admissionStats storeAdmissionStats,
) {
	if e.cumL0WriteBytes == 0 {
		e.cumStoreAdmissionStats = admissionStats
		e.cumL0WriteBytes = l0Metrics.BytesFlushed
		e.cumL0IngestedBytes = l0Metrics.BytesIngested
		e.cumLSMIngestedBytes = cumLSMIngestedBytes
		return
	}
	intL0WriteBytes := int64(l0Metrics.BytesFlushed) - int64(e.cumL0WriteBytes)
	intL0IngestedBytes := int64(l0Metrics.BytesIngested) - int64(e.cumL0IngestedBytes)
	intL0IgnoredIngestedBytes := int64(admissionStats.statsToIgnore.ApproxIngestedIntoL0Bytes) -
		int64(e.cumStoreAdmissionStats.statsToIgnore.ApproxIngestedIntoL0Bytes)
	adjustedIntL0IngestedBytes := intL0IngestedBytes - intL0IgnoredIngestedBytes
	if adjustedIntL0IngestedBytes < 0 {
		adjustedIntL0IngestedBytes = 0
	}
	intWorkCount := int64(admissionStats.admittedCount) -
		int64(e.cumStoreAdmissionStats.admittedCount)
	intL0WriteAccountedBytes :=
		int64(admissionStats.writeAccountedBytes) - int64(e.cumStoreAdmissionStats.writeAccountedBytes)
	// Note that these are not L0 ingested bytes, since we don't know how
	// many did go to L0.
	intIngestedAccountedBytes := int64(admissionStats.ingestedAccountedBytes) -
		int64(e.cumStoreAdmissionStats.ingestedAccountedBytes)
	e.atDoneL0WriteTokensLinearModel.updateModelUsingIntervalStats(
		intL0WriteAccountedBytes, intL0WriteBytes, intWorkCount)
	e.atDoneL0IngestTokensLinearModel.updateModelUsingIntervalStats(
		intIngestedAccountedBytes, adjustedIntL0IngestedBytes, intWorkCount)
	// Ingest across all levels model.
	intLSMIngestedBytes := int64(cumLSMIngestedBytes) - int64(e.cumLSMIngestedBytes)
	intIgnoredIngestedBytes :=
		int64(admissionStats.statsToIgnore.Bytes) - int64(e.cumStoreAdmissionStats.statsToIgnore.Bytes)
	adjustedIntLSMIngestedBytes := intLSMIngestedBytes - intIgnoredIngestedBytes
	if adjustedIntLSMIngestedBytes < 0 {
		adjustedIntLSMIngestedBytes = 0
	}
	e.atDoneIngestTokensLinearModel.updateModelUsingIntervalStats(
		intIngestedAccountedBytes, adjustedIntLSMIngestedBytes, intWorkCount)

	intL0TotalBytes := intL0WriteBytes + adjustedIntL0IngestedBytes
	if intWorkCount > 1 && intL0TotalBytes > 0 {
		// Update the atAdmissionWorkTokens
		intAtAdmissionWorkTokens := intL0TotalBytes / intWorkCount
		const alpha = 0.5
		e.atAdmissionWorkTokens = int64(alpha*float64(intAtAdmissionWorkTokens) +
			(1-alpha)*float64(e.atAdmissionWorkTokens))
		e.atAdmissionWorkTokens = max(1, e.atAdmissionWorkTokens)
	}
	e.aux = perWorkTokensAux{
		intWorkCount:              intWorkCount,
		intL0WriteBytes:           intL0WriteBytes,
		intL0IngestedBytes:        intL0IngestedBytes,
		intLSMIngestedBytes:       intLSMIngestedBytes,
		intL0WriteAccountedBytes:  intL0WriteAccountedBytes,
		intIngestedAccountedBytes: intIngestedAccountedBytes,
		intL0WriteLinearModel:     e.atDoneL0WriteTokensLinearModel.intLinearModel,
		intL0IngestedLinearModel:  e.atDoneL0IngestTokensLinearModel.intLinearModel,
		intIngestedLinearModel:    e.atDoneIngestTokensLinearModel.intLinearModel,
		intBypassedWorkCount: int64(admissionStats.aux.bypassedCount) -
			int64(e.cumStoreAdmissionStats.aux.bypassedCount),
		intL0WriteBypassedAccountedBytes: int64(admissionStats.aux.writeBypassedAccountedBytes) -
			int64(e.cumStoreAdmissionStats.aux.writeBypassedAccountedBytes),
		intIngestedBypassedAccountedBytes: int64(admissionStats.aux.ingestedBypassedAccountedBytes) -
			int64(e.cumStoreAdmissionStats.aux.ingestedBypassedAccountedBytes),
		intL0IgnoredIngestedBytes: intL0IgnoredIngestedBytes,
	}
	// Store the latest cumulative values.
	e.cumStoreAdmissionStats = admissionStats
	e.cumL0WriteBytes = l0Metrics.BytesFlushed
	e.cumL0IngestedBytes = l0Metrics.BytesIngested
	e.cumLSMIngestedBytes = cumLSMIngestedBytes
}

func (e *storePerWorkTokenEstimator) getStoreRequestEstimatesAtAdmission() storeRequestEstimates {
	return storeRequestEstimates{writeTokens: e.atAdmissionWorkTokens}
}

func (e *storePerWorkTokenEstimator) getModelsAtAdmittedDone() (
	l0WriteLM tokensLinearModel,
	l0IngestLM tokensLinearModel,
	ingestLM tokensLinearModel,
) {
	return e.atDoneL0WriteTokensLinearModel.smoothedLinearModel,
		e.atDoneL0IngestTokensLinearModel.smoothedLinearModel,
		e.atDoneIngestTokensLinearModel.smoothedLinearModel
}

func max(i, j int64) int64 {
	if i < j {
		return j
	}
	return i
}
