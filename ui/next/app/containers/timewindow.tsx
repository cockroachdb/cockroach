/// <reference path="../../typings/main.d.ts" />

import * as React from "react";
import { connect } from "react-redux";
import moment = require("moment");

import * as timewindow from "../redux/timewindow";

interface TimeWindowManagerProps {
  // The current timewindow redux state.
  timeWindow: timewindow.TimeWindowState;
  // Callback function used to set a new time window.
  setTimeWindow: (tw: timewindow.TimeWindow) => void;
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
class TimeWindowManager extends React.Component<TimeWindowManagerProps, TimeWindowManagerState> {
  /**
   * checkWindow determines when the current time window will expire. If it is
   * already expired, a new time window is dispatched immediately. Otherwise,
   * setTimeout is used to asynchronously set a new time window when the current
   * one expires.
   */
  checkWindow(props: TimeWindowManagerProps) {
    // Clear any existing timeout.
    if (this.state && this.state.timeout) {
      clearTimeout(this.state.timeout);
    }

    let now = props.now ? props.now() : moment();
    let tw = props.timeWindow.currentWindow;
    let currentEnd = (tw && tw.end) || moment("01-01-1900", "MM-DD-YYYY");
    let expires = currentEnd.clone().add(props.timeWindow.settings.windowValid);
    if (now.isAfter(expires))  {
      // Current time window is expired, reset it.
      this.setWindow(props);
      this.setState({ timeout: null });
    } else {
      // Set a timeout to reset the window when the current window expires.
      let newTimeout = setTimeout(() => this.setWindow(props), expires.diff(now).valueOf());
      this.setState({ timeout: newTimeout });
    }
  }

  /**
   * setWindow dispatches a new time window, extending backwards from the
   * current time.
   */
  setWindow(props: TimeWindowManagerProps) {
    this.setState({ timeout: null });
    let now = props.now ? props.now() : moment();
    props.setTimeWindow({
      start: now.clone().subtract(props.timeWindow.settings.windowSize),
      end: now,
    });
  }

  componentWillMount() {
    this.checkWindow(this.props);
  }

  componentWillReceiveProps(props: TimeWindowManagerProps) {
    this.checkWindow(props);
  }

  render(): any {
    // Render nothing.
    return null;
  }
}

let timeWindowManagerConnected = connect(
  (state) => {
    return {
      timeWindow: state.timewindow,
    };
  },
  {
    setTimeWindow: timewindow.setTimeWindow,
  }
)(TimeWindowManager);

export default timeWindowManagerConnected;
export { TimeWindowManager as TimeWindowManagerUnconnected }
