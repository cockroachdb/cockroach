/**
 * This module maintains a globally-available time window, currently used by all
 * metrics graphs in the ui.
 */

import { Action, PayloadAction } from "../interfaces/action";
import _ = require("lodash");
import moment = require("moment");

export const SET_WINDOW = "cockroachui/timewindow/SET_WINDOW";
export const SET_SETTINGS = "cockroachui/timewindow/SET_SETTINGS";

/**
 * TimeWindow represents an absolute window of time, defined with a start and end
 * time.
 */
export interface TimeWindow {
  start: moment.Moment;
  end: moment.Moment;
}

/**
 * TimeWindowSettings stores the user-specifies time window setting.
 */
export interface TimeWindowSettings {
  // The size of the global time window. Default is ten minutes.
  windowSize: moment.Duration;
  // The length of time the global time window is valid. The current time window
  // is invalid if now > (currentWindow.end + windowValid). Default is ten
  // seconds.
  windowValid: moment.Duration;
}

export class TimeWindowState {
  settings: TimeWindowSettings;
  currentWindow: TimeWindow;
  constructor() {
    this.settings = {
      windowSize: moment.duration(10, "m"),
      windowValid: moment.duration(10, "s"),
    };
  }
}

export default function(state = new TimeWindowState(), action: Action): TimeWindowState {
  switch (action.type) {
    case SET_WINDOW:
      let { payload: tw } = action as PayloadAction<TimeWindow>;
      state = _.clone(state);
      state.currentWindow = tw;
      return state;
    case SET_SETTINGS:
      let { payload: settings } = action as PayloadAction<TimeWindowSettings>;
      state = _.clone(state);
      state.settings = settings;
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

export function setTimeWindowSettings(tws: TimeWindowSettings): PayloadAction<TimeWindowSettings> {
  return {
    type: SET_SETTINGS,
    payload: tws,
  };
}
