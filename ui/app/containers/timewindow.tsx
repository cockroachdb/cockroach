import * as React from "react";
import { connect } from "react-redux";
import moment = require("moment");

import { AdminUIState } from "../redux/state";
import * as timewindow from "../redux/timewindow";

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
class TimeWindowManager extends React.Component<TimeWindowManagerProps, TimeWindowManagerState> {
  constructor() {
    super();
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

    let now = props.now ? props.now() : moment();
    let currentEnd = props.timeWindow.currentWindow.end;
    let expires = currentEnd.clone().add(props.timeWindow.scale.windowValid);
    if (now.isAfter(expires))  {
      // Current time window is expired, reset it.
      this.setWindow(props);
    } else {
      // Set a timeout to reset the window when the current window expires.
      let newTimeout = setTimeout(() => this.setWindow(props), expires.diff(now).valueOf());
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
    let now = props.now ? props.now() : moment();
    props.setTimeWindow({
      start: now.clone().subtract(props.timeWindow.scale.windowSize),
      end: now,
    });
    this.setState({ timeout: null });
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
  (state: AdminUIState) => {
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
