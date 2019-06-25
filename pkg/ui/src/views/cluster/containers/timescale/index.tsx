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
import _ from "lodash";
import moment from "moment";

import "./timescale.styl";

import Dropdown, { DropdownOption, ArrowDirection } from "src/views/shared/components/dropdown";

import { AdminUIState } from "src/redux/state";
import { refreshNodes } from "src/redux/apiReducers";
import * as timewindow from "src/redux/timewindow";
import { LocalSetting } from "src/redux/localsettings";

import { INodeStatus } from "src/util/proto";
import { LongToMoment } from "src/util/convert";

// Tracks whether the default timescale been set once in the app. Tracked across
// the entire app so that changing pages doesn't cause it to reset.
const timescaleDefaultSet = new LocalSetting(
  "timescale/default_set", (s: AdminUIState) => s.localSettings, false,
);

interface TimeRangeProps {
  start: moment.Moment;
  end: moment.Moment;
}

// TimeRange is a prettified string representation of the current timescale.
class TimeRange extends React.Component<TimeRangeProps, {}> {
  render() {
    const s = this.props.start.clone().utc();
    const e = this.props.end.clone().utc();
    const startTimeSpan = <span className="time-range__time">{s.format("HH:mm:ss")}</span>;
    const endTimeSpan = <span className="time-range__time">{e.format("HH:mm:ss")}</span>;
    const startDateSpan = <span className="time-range__date">{s.format("MMM DD, YYYY")}</span>;
    const endDateSpan = <span className="time-range__date">{e.format("MMM DD, YYYY")}</span>;

    const rangeHours = e.diff(s, "hours");
    const pastHours = moment().diff(s, "hours");
    // If start and end times are on the same day, show the times.
    if (rangeHours < 24 && s.date() === e.date()) {
      // If start and end times are today, omit the date, otherwise include it.
      if (pastHours < 24 && s.date() === moment().date()) {
        return <span className="time-range">{startTimeSpan} to {endTimeSpan}</span>;
      } else {
        return <span className="time-range">{startTimeSpan} to {endTimeSpan} on {endDateSpan}</span>;
      }
    }
    // If start and end times are within 48 hours show times and dates,
    // otherwise omit times and only show the dates.
    if ( rangeHours < 48) {
      return <span className="time-range">{startTimeSpan} on {startDateSpan} to {endTimeSpan} on {endDateSpan}</span>;
    } else {
      return <span className="time-range">{startDateSpan} to {endDateSpan}</span>;
    }
  }
}

interface TimeScaleDropdownProps {
  currentScale: timewindow.TimeScale;
  availableScales: timewindow.TimeScaleCollection;
  setTimeScale: typeof timewindow.setTimeScale;
  // Track node data to find the oldest node and set the default timescale.
  refreshNodes: typeof refreshNodes;
  nodeStatuses: INodeStatus[];
  nodeStatusesValid: boolean;
  // Track whether the default has been set.
  setDefaultSet: typeof timescaleDefaultSet.set;
  defaultTimescaleSet: boolean;
}

// TimeScaleDropdown is the dropdown that allows users to select the time range
// for graphs.
class TimeScaleDropdown extends React.Component<TimeScaleDropdownProps, {}> {
  changeSettings = (newTimescaleKey: DropdownOption) => {
    const newSettings = timewindow.availableTimeScales[newTimescaleKey.value];
    if (newSettings) {
      this.props.setTimeScale(newSettings);
    }
  }

  arrowClick = (direction: ArrowDirection) => {
    let selected = _.clone(this.props.currentScale);

    switch (direction) {
      case ArrowDirection.RIGHT:
        if (selected.windowEnd) {
          selected.windowEnd.add(selected.windowSize);
        }
        break;
      case ArrowDirection.LEFT:
        selected.windowEnd = selected.windowEnd || moment();
        selected.windowEnd.subtract(selected.windowSize);
        break;
      default:
        console.error("Unknown direction: ", direction);
    }

    // If the timescale extends into the future then fallback to a default
    // timescale. Otherwise set the key to "Custom" so it appears correctly.
    if (!selected.windowEnd || selected.windowEnd > moment().subtract(selected.windowValid)) {
      selected = _.find(timewindow.availableTimeScales, { windowSize: selected.windowSize });
    } else {
      selected.key = "Custom";
    }

    this.props.setTimeScale(selected);
  }

  getTimescaleOptions = () => {
    const timescaleOptions = _.map(timewindow.availableTimeScales, (_ts, k) => {
      return { value: k, label: "Last " + k };
    });

    // This just ensures that if the key is "Custom" it will show up in the
    // dropdown options. If a custom value isn't currently selected, "Custom"
    // won't show up in the list of options.
    if (!_.has(timewindow.availableTimeScales, this.props.currentScale.key)) {
      timescaleOptions.push({
        value: this.props.currentScale.key,
        label: this.props.currentScale.key,
      });
    }
    return timescaleOptions;
  }

  // Sets the default timescale based on the start time of the oldest node.
  setDefaultTime(props = this.props) {
    if (props.nodeStatusesValid && !props.defaultTimescaleSet) {
      const oldestNode = _.minBy(props.nodeStatuses, (nodeStatus: INodeStatus) => nodeStatus.started_at);
      const clusterStarted = LongToMoment(oldestNode.started_at);
      // TODO (maxlang): This uses the longest uptime, not the oldest
      const clusterDurationHrs = moment.utc().diff(clusterStarted, "hours");
      if (clusterDurationHrs > 1) {
        if (clusterDurationHrs < 6) {
          props.setTimeScale(props.availableScales["1 hour"]);
        } else if (clusterDurationHrs < 12) {
          props.setTimeScale(props.availableScales["6 hours"]);
        }
      }
      props.setDefaultSet(true);
    }
  }

  componentWillMount() {
    this.props.refreshNodes();
    this.setDefaultTime();
  }

  componentWillReceiveProps(props: TimeScaleDropdownProps) {
    if (!props.nodeStatusesValid) {
      this.props.refreshNodes();
    } else {
      this.setDefaultTime(props);
    }
  }

  render() {
    const currentScale = this.props.currentScale;
    let timeRange: React.ReactNode = "";
    const disabledArrows = [];

    // If the time scale doesn't end at the present, display the time range.
    if (currentScale.windowEnd) {
      const start = currentScale.windowEnd.clone().subtract(currentScale.windowSize);
      timeRange = <TimeRange start={start} end={currentScale.windowEnd} />;
    } else {
      // If the time scale does end at the present hide the right arrow.
      disabledArrows.push(ArrowDirection.RIGHT);
    }

    return <div>
      <Dropdown
        title=""
        options={this.getTimescaleOptions()}
        selected={currentScale.key}
        onChange={this.changeSettings}
        onArrowClick={this.arrowClick}
        disabledArrows={disabledArrows}
        />
      <span>{timeRange}</span>
    </div>;
  }
}

export default connect(
  (state: AdminUIState) => {
    return {
      nodeStatusesValid: state.cachedData.nodes.valid,
      nodeStatuses: state.cachedData.nodes.data,
      currentScale: (state.timewindow as timewindow.TimeWindowState).scale,
      availableScales: timewindow.availableTimeScales,
      defaultTimescaleSet: timescaleDefaultSet.selector(state),
    };
  },
  {
    setTimeScale: timewindow.setTimeScale,
    refreshNodes: refreshNodes,
    setDefaultSet: timescaleDefaultSet.set,
  },
)(TimeScaleDropdown);
