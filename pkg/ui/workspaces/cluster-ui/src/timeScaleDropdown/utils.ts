// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import moment from "moment";
import { TimeScale, TimeScaleOption, TimeScaleOptions } from "./timeScaleTypes";

/**
 * defaultTimeScaleOptions is a preconfigured set of time scales that can be
 * selected by the user.
 */
export const defaultTimeScaleOptions: TimeScaleOptions = {
  "Past 10 Minutes": {
    windowSize: moment.duration(10, "minutes"),
    windowValid: moment.duration(10, "seconds"),
    sampleSize: moment.duration(10, "seconds"),
  },
  "Past 30 Minutes": {
    windowSize: moment.duration(30, "minutes"),
    windowValid: moment.duration(30, "seconds"),
    sampleSize: moment.duration(30, "seconds"),
  },
  "Past 1 Hour": {
    windowSize: moment.duration(1, "hour"),
    windowValid: moment.duration(1, "minute"),
    sampleSize: moment.duration(30, "seconds"),
  },
  "Past 6 Hours": {
    windowSize: moment.duration(6, "hours"),
    windowValid: moment.duration(5, "minutes"),
    sampleSize: moment.duration(1, "minutes"),
  },
  "Past 1 Day": {
    windowSize: moment.duration(1, "day"),
    windowValid: moment.duration(10, "minutes"),
    sampleSize: moment.duration(5, "minutes"),
  },
  "Past 2 Days": {
    windowSize: moment.duration(2, "day"),
    windowValid: moment.duration(10, "minutes"),
    sampleSize: moment.duration(5, "minutes"),
  },
  "Past 3 Days": {
    windowSize: moment.duration(3, "day"),
    windowValid: moment.duration(10, "minutes"),
    sampleSize: moment.duration(5, "minutes"),
  },
  "Past Week": {
    windowSize: moment.duration(7, "days"),
    windowValid: moment.duration(10, "minutes"),
    sampleSize: moment.duration(30, "minutes"),
  },
  "Past 2 Weeks": {
    windowSize: moment.duration(14, "days"),
    windowValid: moment.duration(10, "minutes"),
    sampleSize: moment.duration(30, "minutes"),
  },
  "Past Month": {
    windowSize: moment.duration(moment().daysInMonth(), "days"),
    windowValid: moment.duration(20, "minutes"),
    sampleSize: moment.duration(1, "hour"),
  },
  "Past 2 Months": {
    windowSize: moment.duration(moment().daysInMonth() * 2, "days"),
    windowValid: moment.duration(20, "minutes"),
    sampleSize: moment.duration(1, "hour"),
  },
};

export const defaultTimeScaleSelected: TimeScale = {
  ...defaultTimeScaleOptions["Past 1 Hour"],
  key: "Past 1 Hour",
  fixedWindowEnd: false,
};

export const toDateRange = (ts: TimeScale): [moment.Moment, moment.Moment] => {
  const end = ts.fixedWindowEnd
    ? moment.utc(ts.fixedWindowEnd)
    : moment().utc();
  const start = moment.utc(end).subtract(ts.windowSize);
  return [start, end];
};

export const findClosestTimeScale = (
  options: TimeScaleOptions,
  seconds: number,
  startSeconds?: number,
): TimeScaleOption => {
  const data = Object.keys(options).map(
    (val): TimeScaleOption => ({ ...options[val], key: val }),
  );

  data.sort(
    (a, b) =>
      Math.abs(seconds - a.windowSize.asSeconds()) -
      Math.abs(seconds - b.windowSize.asSeconds()),
  );

  const closestWindowSizeSeconds = data[0].windowSize.asSeconds();

  // This logic covers the edge case where drag-to-timerange on a linegraph is of a duration
  // that exactly matches one of the standard available time scales e.g. selecting June 1 at
  // 0:00 to June 2 at 0:00 when the date is July 1 at 0:00 should return a custom timescale
  // instead of past day.
  // If the start is specified, and the window size matches.
  if (startSeconds && closestWindowSizeSeconds === seconds) {
    // Check if the end is before now. If so, it is a custom time.
    const end = moment.unix(startSeconds + seconds);
    if (end < moment()) {
      return { ...data[0], key: "Custom" };
    }
  }

  return closestWindowSizeSeconds === seconds
    ? data[0]
    : { ...data[0], key: "Custom" };
};
