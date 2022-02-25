// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { connect } from "react-redux";
import moment from "moment";

import { AdminUIState } from "src/redux/state";
import * as timewindow from "src/redux/timeScale";
import _ from "lodash";

interface MetricsTimeManagerProps {
  // The current timescale redux state.
  timeScale: timewindow.TimeScaleState;
  // Callback function used to set a new time window.
  setMetricsMovingWindow: typeof timewindow.setMetricsMovingWindow;
  // Optional override method to obtain the current time. Used for tests.
  now?: () => moment.Moment;
}

interface MetricsTimeManagerState {
  // Identifier from an outstanding call to setTimeout.
  timeout: number;
}

/**
 * MetricsTimeManager takes responsibility for advancing the current global
 * time window used by metric graphs. It renders nothing, but will dispatch an
 * updated time window into the redux store whenever the previous time window is
 * expired.
 */
class MetricsTimeManager extends React.Component<
  MetricsTimeManagerProps,
  MetricsTimeManagerState
> {
  constructor(props?: MetricsTimeManagerProps, context?: any) {
    super(props, context);
    this.state = { timeout: null };
  }

  /**
   * checkWindow determines when the metrics current time window will expire. If it is
   * already expired, a new time window is dispatched immediately. Otherwise,
   * setTimeout is used to asynchronously set a new time window when the current
   * one expires.
   */
  checkWindow(props: MetricsTimeManagerProps) {
    // Clear any existing timeout.
    if (this.state.timeout) {
      clearTimeout(this.state.timeout);
      this.setState({ timeout: null });
    }

    // If there is no current window, or if scale have changed since this
    // window was generated, set one immediately.
    if (
      !props.timeScale.metricsTime.currentWindow ||
      props.timeScale.metricsTime.shouldUpdateMetricsWindowFromScale
    ) {
      this.setWindow(props);
      return;
    }

    // Fixed time ranges can't expire.
    if (props.timeScale.scale.fixedWindowEnd) {
      // this.setWindow(props);
      return;
    }

    const now = props.now ? props.now() : moment();
    const currentEnd = props.timeScale.metricsTime.currentWindow.end;
    const expires = currentEnd.clone().add(props.timeScale.scale.windowValid);
    if (now.isAfter(expires)) {
      // Current time window is expired, reset it.
      this.setWindow(props);
    } else {
      // Set a timeout to reset the window when the current window expires.
      const newTimeout = window.setTimeout(
        () => this.setWindow(props),
        expires.diff(now).valueOf(),
      );
      this.setState({
        timeout: newTimeout,
      });
    }
  }

  /**
   * setWindow dispatches a new time window, extending backwards from the
   * current time, by deriving the time from timeScale.
   */
  setWindow(props: MetricsTimeManagerProps) {
    // The following two `if` blocks check two things that in theory should always be in sync.
    // They check if the time selection is a dynamic, moving, now.
    if (!props.timeScale.scale.fixedWindowEnd) {
      if (!props.timeScale.metricsTime.isFixedWindow) {
        const now = props.now ? props.now() : moment();
        // Update the metrics page window with the new "now" time for polling.
        props.setMetricsMovingWindow({
          start: now.clone().subtract(props.timeScale.scale.windowSize),
          end: now,
        });
      }
    } else {
      const fixedWindowEnd = props.timeScale.scale.fixedWindowEnd;
      // Update the metrics page window with the fixed, custom end time.
      props.setMetricsMovingWindow({
        start: fixedWindowEnd
          .clone()
          .subtract(props.timeScale.scale.windowSize),
        end: fixedWindowEnd,
      });
    }
  }

  componentDidMount() {
    this.checkWindow(this.props);
  }

  componentDidUpdate(prevProps: MetricsTimeManagerProps) {
    if (!_.isEqual(prevProps.timeScale, this.props.timeScale)) {
      this.checkWindow(this.props);
    }
  }

  render(): any {
    // Render nothing.
    return null;
  }
}

const metricsTimeManagerConnected = connect(
  (state: AdminUIState) => {
    return {
      timeScale: state.timeScale,
    };
  },
  {
    setMetricsMovingWindow: timewindow.setMetricsMovingWindow,
  },
)(MetricsTimeManager);

export default metricsTimeManagerConnected;
export { MetricsTimeManager as MetricsTimeManagerUnconnected };
