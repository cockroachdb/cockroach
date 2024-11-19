// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

// tokensLinearModel represents a model y = multiplier.x + constant.
type tokensLinearModel struct {
	multiplier float64
	// constant >= 0
	constant int64
}

func (m tokensLinearModel) applyLinearModel(b int64) int64 {
	return int64(float64(b)*m.multiplier) + m.constant
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
// TODO(sumeer): improve the model based on realistic combinations of
// workloads (e.g. foreground writes + index backfills).
type tokensLinearModelFitter struct {
	// [multiplierMin, multiplierMax] constrains the multiplier.
	multiplierMin float64
	multiplierMax float64

	intLinearModel                tokensLinearModel
	smoothedLinearModel           tokensLinearModel
	smoothedPerWorkAccountedBytes int64

	// Should be set to true for the L0 ingested bytes model: if all bytes are
	// ingested below L0, the actual bytes will be zero and the accounted bytes
	// non-zero. We need to update the model in this case.
	updateWithZeroActualNonZeroAccountedForL0IngestedModel bool
}

func makeTokensLinearModelFitter(
	multMin float64, multMax float64, updateWithZeroActualNonZeroAccountedForL0IngestedModel bool,
) tokensLinearModelFitter {
	return tokensLinearModelFitter{
		multiplierMin: multMin,
		multiplierMax: multMax,
		smoothedLinearModel: tokensLinearModel{
			multiplier: (multMin + multMax) / 2,
			constant:   1,
		},
		smoothedPerWorkAccountedBytes:                          1,
		updateWithZeroActualNonZeroAccountedForL0IngestedModel: updateWithZeroActualNonZeroAccountedForL0IngestedModel,
	}
}

// updateModelUsingIntervalStats updates the model, based on various stats
// over the last interval: the number of work items admitted (workCount), the
// bytes claimed by these work items (accountedBytes), and the actual bytes
// observed in the LSM for that interval (actualBytes).
//
// As mentioned store_token_estimation.go, the current fitting algorithm is
// probably poor, though an improvement on what we had previously. The approach
// taken is:
//
//   - Fit the best model we can for the interval,
//     multiplier*accountedBytes + workCount*constant = actualBytes, while
//     minimizing the constant. We prefer the model to use the multiplier for
//     most of what it needs to account for actualBytes.
//     This exact model ignores inaccuracies due to integer arithmetic -- we
//     don't care about rounding errors since an error of 2 bytes per request is
//     inconsequential.
//
//   - The multiplier has to conform to the [min,max] configured for this model,
//     and constant has to conform to a value >= 1. The constant is constrained
//     to be >=1 on the intuition that we want a request to consume at least 1
//     token -- it isn't clear that this intuition is meaningful in any way.
//
//   - Exponentially smooth this exact model's multiplier and constant based on
//     history.
func (f *tokensLinearModelFitter) updateModelUsingIntervalStats(
	accountedBytes int64, actualBytes int64, workCount int64,
) {
	if workCount <= 1 || (actualBytes <= 0 &&
		(!f.updateWithZeroActualNonZeroAccountedForL0IngestedModel || accountedBytes <= 0)) {
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
