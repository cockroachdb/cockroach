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
import { defaultTimeScaleOptions } from "./utils";

/**
 * TimeWindow represents an absolute window of time, defined with a start and
 * end time.
 */
export interface TimeWindow {
  start: moment.Moment;
  end: moment.Moment;
}

/**
 * TimeScale describes the requested dimensions of TimeWindows; it
 * prescribes a length for the window, along with a period of time that a
 * newly created TimeWindow will remain valid.
 */
export interface TimeScale {
  // The key used to index in to the availableTimeScales collection.
  key?: string;
  // The size of a global time window. Default is ten minutes.
  windowSize: moment.Duration;
  // The length of time the global time window is valid. The current time window
  // is invalid if now > (currentWindow.end + windowValid). Default is ten
  // seconds. If windowEnd is set this is ignored.
  windowValid?: moment.Duration;
  // The expected duration of individual samples for queries at this time scale.
  sampleSize: moment.Duration;
  // The end time of the window, or null if it should be a dynamically moving "now"
  windowEnd: moment.Moment | null;
}

export class TimeWindowState {
  // Currently selected scale.
  scale: TimeScale;
  constructor() {
    this.scale = {
      ...defaultTimeScaleOptions["Past 10 Minutes"],
      windowEnd: null,
      key: "Past 10 Minutes",
    };
  }
}

export type DefaultTimeScaleOption = Omit<TimeScale, "windowEnd">;

export interface DefaultTimesScaleOptions {
  [key: string]: DefaultTimeScaleOption;
}

export type TimeRangeTitle =
  | {
      dateStart: string;
      dateEnd: string;
      timeStart: string;
      timeEnd: string;
      title: "Custom";
      timeLabel: string;
    }
  | { title: string; timeLabel: string };

export enum ArrowDirection {
  LEFT,
  RIGHT,
  CENTER,
}
