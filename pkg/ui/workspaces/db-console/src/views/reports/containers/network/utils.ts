// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import { deviation as d3Deviation, mean as d3Mean } from "d3";
import _ from "lodash";
import { StdDev } from "./latency";

// getMean returns the mean of the array of numbers.
export const getMean = (latencies: number[]): number => d3Mean(latencies);

// createStdDev creates a StdDev.
export const createStdDev = (latencies: number[]): StdDev => {
  let stddev = d3Deviation(latencies);
  if (_.isUndefined(stddev)) {
    stddev = 0;
  }
  const mean = getMean(latencies);
  const stddevPlus1 = stddev > 0 ? mean + stddev : 0;
  const stddevPlus2 = stddev > 0 ? stddevPlus1 + stddev : 0;
  const stddevMinus1 = stddev > 0 ? _.max([mean - stddev, 0]) : 0;
  const stddevMinus2 = stddev > 0 ? _.max([stddevMinus1 - stddev, 0]) : 0;
  return {
    stddev,
    stddevMinus2,
    stddevMinus1,
    stddevPlus1,
    stddevPlus2,
  };
};
