/**
 * This module maintains a globally-available time window, currently used by all
 * metrics graphs in the ui.
 */

import { Action, PayloadAction } from "../interfaces/action";
import _ = require("lodash");
import moment = require("moment");

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
  // The size of a global time window. Default is ten minutes.
  windowSize: moment.Duration;
  // The length of time the global time window is valid. The current time window
  // is invalid if now > (currentWindow.end + windowValid). Default is ten
  // seconds.
  windowValid: moment.Duration;
}

export interface TimeScaleCollection {
  [key: string]: TimeScale;
}

/**
 * availableTimeScales is a preconfigured set of time scales that can be
 * selected by the user.
 */
export let availableTimeScales: TimeScaleCollection = {
  "10 min": {
    windowSize: moment.duration(10, "minutes"),
    windowValid: moment.duration(10, "seconds"),
  },
  "1 hour": {
    windowSize: moment.duration(1, "hour"),
    windowValid: moment.duration(1, "minute"),
  },
  "6 hours": {
    windowSize: moment.duration(6, "hours"),
    windowValid: moment.duration(5, "minutes"),
  },
  "12 hours": {
    windowSize: moment.duration(12, "hours"),
    windowValid: moment.duration(10, "minutes"),
  },
  "1 day": {
    windowSize: moment.duration(1, "day"),
    windowValid: moment.duration(10, "minutes"),
  },
};

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

export default function(state = new TimeWindowState(), action: Action): TimeWindowState {
  switch (action.type) {
    case SET_WINDOW:
      let { payload: tw } = action as PayloadAction<TimeWindow>;
      state = _.clone(state);
      state.currentWindow = tw;
      state.scaleChanged = false;
      return state;
    case SET_SCALE:
      let { payload: scale } = action as PayloadAction<TimeScale>;
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
