// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

/**
 * This module maintains a globally-available time window, currently used by all
 * metrics graphs in the ui.
 */

import { Action } from "redux";
import { PayloadAction } from "src/interfaces/action";
import _ from "lodash";
import moment from "moment";

export const SET_WINDOW = "cockroachui/timewindow/SET_WINDOW";
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
export let availableTimeScales: TimeScaleCollection = _.mapValues(
  {
    "10 min": {
      windowSize: moment.duration(10, "minutes"),
      windowValid: moment.duration(10, "seconds"),
      sampleSize: moment.duration(10, "seconds"),
    },
    "1 hour": {
      windowSize: moment.duration(1, "hour"),
      windowValid: moment.duration(1, "minute"),
      sampleSize: moment.duration(30, "seconds"),
    },
    "6 hours": {
      windowSize: moment.duration(6, "hours"),
      windowValid: moment.duration(5, "minutes"),
      sampleSize: moment.duration(1, "minutes"),
    },
    "12 hours": {
      windowSize: moment.duration(12, "hours"),
      windowValid: moment.duration(10, "minutes"),
      sampleSize: moment.duration(2, "minutes"),
    },
    "1 day": {
      windowSize: moment.duration(1, "day"),
      windowValid: moment.duration(10, "minutes"),
      sampleSize: moment.duration(5, "minutes"),
    },
    "1 week": {
      windowSize: moment.duration(7, "days"),
      windowValid: moment.duration(10, "minutes"),
      sampleSize: moment.duration(30, "minutes"),
    },
    "1 month": {
      windowSize: moment.duration(1, "month"),
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

export class TimeWindowState {
  // Currently selected scale.
  scale: TimeScale;
  // Currently established time window.
  currentWindow: TimeWindow;
  // True if scale has changed since currentWindow was generated.
  scaleChanged: boolean;
  constructor() {
    this.scale = availableTimeScales["10 min"];
  }
}

export function timeWindowReducer(state = new TimeWindowState(), action: Action): TimeWindowState {
  switch (action.type) {
    case SET_WINDOW:
      const { payload: tw } = action as PayloadAction<TimeWindow>;
      state = _.clone(state);
      state.currentWindow = tw;
      state.scaleChanged = false;
      return state;
    case SET_SCALE:
      const { payload: scale } = action as PayloadAction<TimeScale>;
      state = _.clone(state);
      state.scale = scale;
      state.scaleChanged = true;
      return state;
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

export function setTimeScale(ts: TimeScale): PayloadAction<TimeScale> {
  return {
    type: SET_SCALE,
    payload: ts,
  };
}
