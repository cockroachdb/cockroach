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

export const SET_SCALE = "cockroachui/timewindow/SET_SCALE";
export const SET_METRICS_MOVING_WINDOW =
  "cockroachui/timewindow/SET_METRICS_MOVING_WINDOW";
export const SET_METRICS_FIXED_WINDOW =
  "cockroachui/timewindow/SET_METRICS_FIXED_WINDOW";

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
  /**
   * The key used to index in to the defaultTimeScaleOptions collection.
   * The key is "Custom" when specifying a custom time that is not one of the default options
   */
  key?: string;
  // The size of a global time window. Default is ten minutes.
  windowSize: moment.Duration;
  // The length of time the global time window is valid. The current time window
  // is invalid if now > (metricsTime.currentWindow.end + windowValid). Default is ten
  // seconds. If fixedWindowEnd is set this is ignored.
  windowValid?: moment.Duration;
  // The expected duration of individual samples for queries at this time scale.
  sampleSize: moment.Duration;
  /**
   * The fixed end time of the window, or false if it should be a dynamically moving "now".
   * Typically, when the `key` property is a default option, `fixedWindowEnd` should be false.
   * And when the `key` property is "Custom" `fixedWindowEnd` should be a specific Moment.
   * It is unclear if there are legitimate reasons for the two being out of sync.
   */
  fixedWindowEnd: moment.Moment | false;
}

export class TimeScaleState {
  // Currently selected scale.
  scale: TimeScale;
  /**
   * Timekeeping for the db console metrics page. Due to tech debt, this duplicates part of state currently in scale.
   * e.g.,
   *  currentWindow.start can be derived from metricsTime.currentWindow.end - scale.windowSize,
   *  and metricsTime.isFixedWindow can be derived from scale.
   * However, the key difference is that metricsTime.currentWindow.end is different from scale.fixedWindowEnd.
   *  Specifically, when the end is "now", scale.fixedWindowEnd is false and metricsTime.currentWindow.end is a specific
   *  time that is continually updated in order to do polling.
   */
  metricsTime: {
    // Start and end times to be used for metrics page graphs.
    currentWindow: TimeWindow;
    // True if scale has changed since currentWindow was generated, and it should be re-generated from scale.
    shouldUpdateMetricsWindowFromScale: boolean;
    // True if currentWindow should be unchanging. False if it should be updated with new "now" times.
    isFixedWindow: boolean;
  };

  constructor() {
    this.scale = {
      ...defaultTimeScaleOptions["Past 10 Minutes"],
      key: "Past 10 Minutes",
      fixedWindowEnd: false,
    };
    this.metricsTime = {
      // This is explicitly initialized as undefined to match the prior implementation while satisfying Typescript.
      currentWindow: undefined,
      isFixedWindow: false,
      // This is used to update the metrics time window after the scale is changed, and prevent cycles when directly
      // updating the metrics time window.
      shouldUpdateMetricsWindowFromScale: false,
    };
  }
}

export function timeScaleReducer(
  state = new TimeScaleState(),
  action: Action,
): TimeScaleState {
  switch (action.type) {
    case SET_SCALE: {
      const { payload: scale } = action as PayloadAction<TimeScale>;
      state = _.cloneDeep(state);
      if (scale.key === "Custom") {
        state.metricsTime.isFixedWindow = true;
      } else {
        state.metricsTime.isFixedWindow = false;
      }
      state.scale = scale;
      state.metricsTime.shouldUpdateMetricsWindowFromScale = true;
      return state;
    }
    case SET_METRICS_MOVING_WINDOW: {
      const { payload: tw } = action as PayloadAction<TimeWindow>;
      state = _.cloneDeep(state);
      state.metricsTime.currentWindow = tw;
      state.metricsTime.shouldUpdateMetricsWindowFromScale = false;
      return state;
    }
    case SET_METRICS_FIXED_WINDOW: {
      const { payload: data } = action as PayloadAction<TimeWindow>;
      state = _.cloneDeep(state);
      state.metricsTime.currentWindow = data;
      state.metricsTime.isFixedWindow = true;
      state.metricsTime.shouldUpdateMetricsWindowFromScale = false;
      return state;
    }
    default:
      return state;
  }
}

export function setTimeScale(ts: TimeScale): PayloadAction<TimeScale> {
  return {
    type: SET_SCALE,
    payload: ts,
  };
}

export function setMetricsMovingWindow(
  tw: TimeWindow,
): PayloadAction<TimeWindow> {
  return {
    type: SET_METRICS_MOVING_WINDOW,
    payload: tw,
  };
}

export function setMetricsFixedWindow(
  tw: TimeWindow,
): PayloadAction<TimeWindow> {
  return {
    type: SET_METRICS_FIXED_WINDOW,
    payload: tw,
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
