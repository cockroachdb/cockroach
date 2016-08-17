/**
 * HACK
 *
 * This enumeration is copied due to an incompatibility between two tools in our
 * system:
 *
 * 1. The systemJS typescript loader (frankwallis/plugin-typescript) compiles
 * files using typescript's "single file compilation" mode, which is unable to
 * resolve ambiently declared const enums:
 *
 *     https://github.com/frankwallis/plugin-typescript/issues/89
 *
 * 2. The Proto2Typescript generator (SINTEF-9012/Proto2TypeScript) outputs
 * enumerations as ambiently declared const enums in a .d.ts file:
 *
 *    https://github.com/SINTEF-9012/Proto2TypeScript/issues/4
 *
 * Unfortunately, it is not trivial to change either of these behaviors; the
 * plugin-typescript behavior is unfixable (fundamentally incompatible with its
 * current basic strategy), while the Proto2Typescript generated file would need
 * to be changed dramatically; specifically, it would need generate an
 * importable module format (rather than its current design of declaring ambient
 * global objects).
 *
 * As an aside, it is possible we will eventually stop using Proto2TypeScript:
 *
 *     https://github.com/cockroachdb/cockroach/issues/6616
 *
 */

/**
 * TimeSeriesQueryAggregator is an enumeration used by Cockroach's time series
 * query system, used to select an aggregator function. This must be kept
 * manually in sync with the same enumeration in `generated/protos.d.ts`.
 */
export const enum TimeSeriesQueryAggregator {
  AVG = 1,
  SUM = 2,
  MAX = 3,
  MIN = 4,
}

/**
 * TimeSeriesQueryDerivative is an enumeration used by Cockroach's time series
 * query system, used to select an derivated function. This must be kept
 * manually in sync with the same enumeration in `generated/protos.d.ts`.
 */
export const enum TimeSeriesQueryDerivative {
  NONE = 0,
  DERIVATIVE = 1,
  NON_NEGATIVE_DERIVATIVE = 2,
}

/**
 * Severity is an enumeration used by Cockroach's logs to indicate the log level
 * of a particular message. This must be kept manually in sync with the same
 * enumeration in `generated/protos.d.ts`.
 */
export enum Severity {
  UNKNOWN = 0,
  INFO = 1,
  WARNING = 2,
  ERROR = 3,
  FATAL = 4,
  NONE = 5,
}
