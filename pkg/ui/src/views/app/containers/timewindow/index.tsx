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
import * as timewindow from "src/redux/timewindow";
import _ from "lodash";

interface TimeWindowManagerProps {
  // The current timewindow redux state.
  timeWindow: timewindow.TimeWindowState;
  // Callback function used to set a new time window.
  setTimeWindow: typeof timewindow.setTimeWindow;
  // Optional override method to obtain the current time. Used for tests.
  now?: () => moment.Moment;
}

interface TimeWindowManagerState {
  // Identifier from an outstanding call to setTimeout.
  timeout: number;
}

/**
 * TimeWindowManager takes responsibility for advancing the current global
 * time window used by metric graphs. It renders nothing, but will dispatch an
 * updated time window into the redux store whenever the previous time window is
 * expired.
 */
class TimeWindowManager extends React.Component<
  TimeWindowManagerProps,
  TimeWindowManagerState
> {
  constructor(props?: TimeWindowManagerProps, context?: any) {
    super(props, context);
    this.state = { timeout: null };
  }

  /**
   * checkWindow determines when the current time window will expire. If it is
   * already expired, a new time window is dispatched immediately. Otherwise,
   * setTimeout is used to asynchronously set a new time window when the current
   * one expires.
   */
  checkWindow(props: TimeWindowManagerProps) {
    // Clear any existing timeout.
    if (this.state.timeout) {
      clearTimeout(this.state.timeout);
      this.setState({ timeout: null });
    }

    // If there is no current window, or if scale have changed since this
    // window was generated, set one immediately.
    if (!props.timeWindow.currentWindow || props.timeWindow.scaleChanged) {
      this.setWindow(props);
      return;
    }

    // Exact time ranges can't expire.
    if (props.timeWindow.scale.windowEnd) {
      // this.setWindow(props);
      return;
    }

    const now = props.now ? props.now() : moment();
    const currentEnd = props.timeWindow.currentWindow.end;
    const expires = currentEnd.clone().add(props.timeWindow.scale.windowValid);
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
   * current time.
   */
  setWindow(props: TimeWindowManagerProps) {
    if (!props.timeWindow.scale.windowEnd) {
      if (!props.timeWindow.useTimeRange) {
        const now = props.now ? props.now() : moment();
        props.setTimeWindow({
          start: now.clone().subtract(props.timeWindow.scale.windowSize),
          end: now,
        });
      }
    } else {
      const windowEnd = props.timeWindow.scale.windowEnd;
      props.setTimeWindow({
        start: windowEnd.clone().subtract(props.timeWindow.scale.windowSize),
        end: windowEnd,
      });
    }
  }

  componentDidMount() {
    this.checkWindow(this.props);
  }

  componentDidUpdate(prevProps: TimeWindowManagerProps) {
    if (!_.isEqual(prevProps.timeWindow, this.props.timeWindow)) {
      this.checkWindow(this.props);
    }
  }

  render(): any {
    // Render nothing.
    return null;
  }
}

const timeWindowManagerConnected = connect(
  (state: AdminUIState) => {
    return {
      timeWindow: state.timewindow,
    };
  },
  {
    setTimeWindow: timewindow.setTimeWindow,
  },
)(TimeWindowManager);

export default timeWindowManagerConnected;
export { TimeWindowManager as TimeWindowManagerUnconnected };
