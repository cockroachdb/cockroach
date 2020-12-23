// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/**
 * This module maintains a globally-available time window, currently used by all
 * metrics graphs in the ui.
 */

import { Action } from "redux";
import { PayloadAction } from "src/interfaces/action";
import _ from "lodash";
import moment from "moment";

export const SET_WINDOW = "cockroachui/timewindow/SET_WINDOW";
export const SET_RANGE = "cockroachui/timewindow/SET_RANGE";
export const SET_SCALE = "cockroachui/timewindow/SET_SCALE";

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
  // The end time of the window if it isn't the present
  windowEnd?: moment.Moment;
}

export interface TimeScaleCollection {
  [key: string]: TimeScale;
}

/**
 * availableTimeScales is a preconfigured set of time scales that can be
 * selected by the user.
 */
export const availableTimeScales: TimeScaleCollection = _.mapValues(
  {
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
  },
  (v, k) => {
    // This weirdness is to work around an apparent issue in TypeScript:
    // https://github.com/Microsoft/TypeScript/issues/20305
    const result: TimeScale = v;
    // Set the "key" attribute.
    result.key = k;
    return result;
  },
);

export const findClosestTimeScale = (seconds: number) => {
  const data: TimeScale[] = [];
  Object.keys(availableTimeScales).forEach((val) =>
    data.push(availableTimeScales[val]),
  );
  data.sort(
    (a, b) =>
      Math.abs(seconds - a.windowSize.asSeconds()) -
      Math.abs(seconds - b.windowSize.asSeconds()),
  );
  return data[0].windowSize.asSeconds() === seconds
    ? data[0]
    : { ...data[0], key: "Custom" };
};

export class TimeWindowState {
  // Currently selected scale.
  scale: TimeScale;
  // Currently established time window.
  currentWindow: TimeWindow;
  // True if scale has changed since currentWindow was generated.
  scaleChanged: boolean;
  useTimeRange: boolean;
  constructor() {
    this.scale = availableTimeScales["Past 10 Minutes"];
    this.useTimeRange = false;
    this.scaleChanged = false;
  }
}

export function timeWindowReducer(
  state = new TimeWindowState(),
  action: Action,
): TimeWindowState {
  switch (action.type) {
    case SET_WINDOW: {
      const { payload: tw } = action as PayloadAction<TimeWindow>;
      state = _.clone(state);
      state.currentWindow = tw;
      state.scaleChanged = false;
      return state;
    }
    case SET_RANGE: {
      const { payload: data } = action as PayloadAction<TimeWindow>;
      state = _.clone(state);
      state.currentWindow = data;
      state.useTimeRange = true;
      state.scaleChanged = false;
      return state;
    }
    case SET_SCALE: {
      const { payload: scale } = action as PayloadAction<TimeScale>;
      state = _.clone(state);
      if (scale.key === "Custom") {
        state.useTimeRange = true;
      } else {
        state.useTimeRange = false;
      }
      state.scale = scale;
      state.scaleChanged = true;
      return state;
    }
    default:
      return state;
  }
}

export function setTimeWindow(tw: TimeWindow): PayloadAction<TimeWindow> {
  return {
    type: SET_WINDOW,
    payload: tw,
  };
}

export function setTimeRange(tw: TimeWindow): PayloadAction<TimeWindow> {
  return {
    type: SET_RANGE,
    payload: tw,
  };
}

export function setTimeScale(ts: TimeScale): PayloadAction<TimeScale> {
  return {
    type: SET_SCALE,
    payload: ts,
  };
}
