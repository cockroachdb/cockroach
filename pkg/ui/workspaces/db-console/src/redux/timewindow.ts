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
import { defaultTimeScaleOptions } from "@cockroachlabs/cluster-ui";
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
  // The key used to index in to a TimeScaleCollection.
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
  // Timekeeping for the db console metrics page
  metricsTime: {
    // Currently established time window.
    currentWindow: TimeWindow;
    // True if scale has changed since currentWindow was generated.
    scaleChanged: boolean;
    useTimeRange: boolean;
  };

  constructor() {
    this.scale = {
      ...defaultTimeScaleOptions["Past 10 Minutes"],
      key: "Past 10 Minutes",
      windowEnd: null,
    };
    this.metricsTime = {
      // historically, this is initialized as undefined
      currentWindow: undefined,
      useTimeRange: false,
      // this is used to update the metrics time window after the scale is changed, and prevent cycles when directly updating the metrics time window
      scaleChanged: false,
    };
  }
}

export function timeWindowReducer(
  state = new TimeWindowState(),
  action: Action,
): TimeWindowState {
  switch (action.type) {
    case SET_WINDOW: {
      const { payload: tw } = action as PayloadAction<TimeWindow>;
      state = _.cloneDeep(state);
      state.metricsTime.currentWindow = tw;
      state.metricsTime.scaleChanged = false;
      return state;
    }
    case SET_RANGE: {
      const { payload: data } = action as PayloadAction<TimeWindow>;
      state = _.cloneDeep(state);
      state.metricsTime.currentWindow = data;
      state.metricsTime.useTimeRange = true;
      state.metricsTime.scaleChanged = false;
      return state;
    }
    case SET_SCALE: {
      const { payload: scale } = action as PayloadAction<TimeScale>;
      state = _.cloneDeep(state);
      if (scale.key === "Custom") {
        state.metricsTime.useTimeRange = true;
      } else {
        state.metricsTime.useTimeRange = false;
      }
      state.scale = scale;
      state.metricsTime.scaleChanged = true;
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

export type AdjustTimeScaleReturnType = {
  timeScale: TimeScale;
  adjustmentReason?: "low_resolution_period" | "deleted_data_period";
};

/*
 * Cluster stores metrics data for some defined period of time and then rolls up data into lower resolution
 * and then removes it. Following shows possible cases when for some date ranges it isn't possible to request
 * time series with 10s resolution.
 *
 *  (removed)  (stores data with 30min resolution)  (stores with 10s res)
 * -----------X-----------------------------------X-----------------------X------>
 * [resolution30mStorageTTL]           [resolution10sStorageTTL]        [now]
 *
 * - time series older than resolution30mStorageTTL duration is subject for deletion
 * - time series before resolution30mStorageTTL and older than resolution10sStorageTTL is stored with
 * 30 min resolution only. 10s resolution data is removed for this period.
 * - time series before resolution10sStorageTTL is stored with 10s resolution
 *
 * adjustTimeScale function checks whether selected timeWindow and provided timeScale allow request data
 * with described above restrictions.
 */
export const adjustTimeScale = (
  curTimeScale: TimeScale,
  timeWindow: TimeWindow,
  resolution10sStorageTTL: moment.Duration,
  resolution30mStorageTTL: moment.Duration,
): AdjustTimeScaleReturnType => {
  const result: AdjustTimeScaleReturnType = {
    timeScale: {
      ...curTimeScale,
    },
  };
  if (
    !resolution30mStorageTTL ||
    !resolution10sStorageTTL ||
    !curTimeScale ||
    !timeWindow
  ) {
    return result;
  }
  const now = moment().utc();
  const ttl10secDate = now.subtract(resolution10sStorageTTL);
  const isOutsideOf10sResolution = timeWindow.start.isBefore(ttl10secDate);
  const isSmallerSampleSize =
    curTimeScale.sampleSize.asSeconds() <= moment.duration(30, "m").asSeconds();

  if (isOutsideOf10sResolution && isSmallerSampleSize) {
    result.timeScale.sampleSize = moment.duration(30, "minutes");
    result.adjustmentReason = "low_resolution_period";
  }

  const resolution30minDate = now.subtract(resolution30mStorageTTL);
  const isOutsideOf30minResolution = timeWindow.start.isBefore(
    resolution30minDate,
  );
  if (isOutsideOf30minResolution) {
    result.adjustmentReason = "deleted_data_period";
  }
  return result;
};
