// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";
import moment from "moment";
import { queryByName, queryToObj, queryToString } from "oss/src/util/query";
import React from "react";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { refreshNodes } from "src/redux/apiReducers";
import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import * as timewindow from "src/redux/timewindow";
import { LongToMoment } from "src/util/convert";
import { INodeStatus } from "src/util/proto";
import Dropdown, { ArrowDirection, DropdownOption } from "src/views/shared/components/dropdown";
import TimeFrameControls from "../../components/controls";
import RangeSelect, { DateTypes } from "../../components/range";
import "./timescale.styl";
import { Divider } from "antd";

// Tracks whether the default timescale been set once in the app. Tracked across
// the entire app so that changing pages doesn't cause it to reset.
const timescaleDefaultSet = new LocalSetting(
  "timescale/default_set", (s: AdminUIState) => s.localSettings, false,
);

interface TimeScaleDropdownProps extends RouteComponentProps {
  currentScale: timewindow.TimeScale;
  currentWindow: timewindow.TimeWindow;
  availableScales: timewindow.TimeScaleCollection;
  setTimeScale: typeof timewindow.setTimeScale;
  setTimeRange: typeof timewindow.setTimeRange;
  // Track node data to find the oldest node and set the default timescale.
  refreshNodes: typeof refreshNodes;
  nodeStatuses: INodeStatus[];
  nodeStatusesValid: boolean;
  // Track whether the default has been set.
  setDefaultSet: typeof timescaleDefaultSet.set;
  defaultTimescaleSet: boolean;
  useTimeRange: boolean;
}

export const getTimeLabel = (currentWindow?: timewindow.TimeWindow, windowSize?: moment.Duration) => {
  const time = windowSize ? windowSize : moment.duration(moment(currentWindow.end).diff(currentWindow.start));
  const seconds = time.asSeconds();
  const minutes = 60;
  const hour = minutes * 60;
  const day = hour * 24;
  const week = day * 7;
  const month = day * moment().daysInMonth();
  switch (true) {
    case seconds < hour:
      return time.asMinutes().toFixed() + "m";
    case (seconds >= hour && seconds < day):
      return time.asHours().toFixed() + "h";
    case seconds < week:
      return time.asDays().toFixed() + "d";
    case seconds < month:
      return time.asWeeks().toFixed() + "w";
    default:
      return time.asMonths().toFixed() + "m";
  }
};

// TimeScaleDropdown is the dropdown that allows users to select the time range
// for graphs.
class TimeScaleDropdown extends React.Component<TimeScaleDropdownProps, {}> {
  changeSettings = (newTimescaleKey: DropdownOption) => {
    const newSettings = timewindow.availableTimeScales[newTimescaleKey.value];
    newSettings.windowEnd = null;
    if (newSettings) {
      this.setQueryParamsByDates(newSettings.windowSize, moment());
      this.props.setTimeScale(newSettings);
    }
  }

  arrowClick = (direction: ArrowDirection) => {
    const { currentWindow, currentScale } = this.props;
    const windowSize: any = moment.duration(moment(currentWindow.end).diff(currentWindow.start));
    const seconds = windowSize.asSeconds();
    let selected = {};
    let key = currentScale.key;
    let windowEnd = currentWindow.end || moment.utc();

    switch (direction) {
      case ArrowDirection.RIGHT:
        if (windowEnd) {
          windowEnd = windowEnd.add(seconds, "seconds");
        }
        break;
      case ArrowDirection.LEFT:
        windowEnd = windowEnd.subtract(seconds, "seconds");
        break;
      case ArrowDirection.CENTER:
        windowEnd = moment.utc();
        break;
      default:
        console.error("Unknown direction: ", direction);
    }
    // If the timescale extends into the future then fallback to a default
    // timescale. Otherwise set the key to "Custom" so it appears correctly.
    if (!windowEnd || windowEnd > moment().subtract(currentScale.windowValid)) {
      if (_.find(timewindow.availableTimeScales, { windowSize: { _data: windowSize._data } } as any)) {
        selected = (_.find(timewindow.availableTimeScales, { windowSize: { _data: windowSize._data } } as any));
      } else {
        key = "Custom";
      }
    } else {
      key = "Custom";
    }

    this.setQueryParamsByDates(windowSize, windowEnd);
    this.props.setTimeScale({ ...currentScale, windowEnd, windowSize, key, ...selected });
  }

  getTimescaleOptions = () => {
    const { currentWindow } = this.props;
    const timescaleOptions = _.map(timewindow.availableTimeScales, (_ts, k) => {
      return { value: k, label: k, timeLabel: getTimeLabel(null, _ts.windowSize) };
    });

    // This just ensures that if the key is "Custom" it will show up in the
    // dropdown options. If a custom value isn't currently selected, "Custom"
    // won't show up in the list of options.
    timescaleOptions.push({
      value: "Custom",
      label: "Custom",
      timeLabel: getTimeLabel(currentWindow),
    });
    return timescaleOptions;
  }

  // Sets the default timescale based on the start time of the oldest node.
  setDefaultTime(props: TimeScaleDropdownProps = this.props) {
    if (props.nodeStatusesValid && !props.defaultTimescaleSet) {
      const oldestNode = _.minBy(props.nodeStatuses, (nodeStatus: INodeStatus) => nodeStatus.started_at);
      const clusterStarted = LongToMoment(oldestNode.started_at);
      // TODO (maxlang): This uses the longest uptime, not the oldest
      const clusterDurationHrs = moment.utc().diff(clusterStarted, "hours");
      if (clusterDurationHrs > 1) {
        if (clusterDurationHrs < 6) {
          props.setTimeScale(props.availableScales["Past 1 Hour"]);
        } else if (clusterDurationHrs < 12) {
          props.setTimeScale(props.availableScales["Past 6 Hours"]);
        }
      }
      props.setDefaultSet(true);
    }
  }

  componentWillMount() {
    this.props.refreshNodes();
    this.setDefaultTime();
  }

  componentDidMount() {
    this.getQueryParams();
  }

  getQueryParams = () => {
    const { location } = this.props;
    const queryStart = queryByName(location, "start");
    const queryEnd = queryByName(location, "end");
    const start = queryStart && moment.unix(Number(queryStart)).utc();
    const end = queryEnd && moment.unix(Number(queryEnd)).utc();

    if (start || end) {
      this.setDatesByQueryParams({ start, end });
    }
  }

  setQueryParams = (date: moment.Moment, type: DateTypes) => {
    const { location, history } = this.props;
    const dataType = type === DateTypes.DATE_FROM ? "start" : "end";
    const timestamp = moment(date).format("X");
    const query = queryToObj(location, dataType, timestamp);
    history.push({
      pathname: location.pathname,
      search: `?${queryToString(query)}`,
    });
  }

  componentWillReceiveProps(props: TimeScaleDropdownProps) {
    if (!props.nodeStatusesValid) {
      this.props.refreshNodes();
    } else if (!props.useTimeRange) {
      this.setDefaultTime(props);
    }
  }

  setQueryParamsByDates = (duration: moment.Duration, dateEnd: moment.Moment) => {
    const { location, history } = this.props;
    const { pathname, search } = location;
    const urlParams = new URLSearchParams(search);
    const seconds = duration.clone().asSeconds();
    const end = dateEnd.clone();
    const start =  moment.utc(end.subtract(seconds, "seconds")).format("X");

    urlParams.set("start", start);
    urlParams.set("end", moment.utc(dateEnd).format("X"));

    history.push({
      pathname,
      search: urlParams.toString(),
    });
  }

  setDatesByQueryParams = (dates: timewindow.TimeWindow) => {
    const selected = _.clone(this.props.currentScale);
    const end  = dates.end || moment().set({hours: 23, minutes: 59, seconds: 0});
    const start = dates.start || moment().set({hours: 0, minutes: 0, seconds: 0});

    selected.key = "Custom";
    this.props.setTimeScale(selected);
    this.props.setTimeRange({ end, start });
  }

  setDate = (date: moment.Moment, type: DateTypes) => {
    const currentWindow = _.clone(this.props.currentWindow);
    const selected = _.clone(this.props.currentScale);
    const end  = currentWindow.end || moment().utc().set({hours: 23, minutes: 59, seconds: 0});
    const start = currentWindow.start || moment().utc().set({hours: 0, minutes: 0, seconds: 0});
    switch (type) {
      case DateTypes.DATE_FROM:
        this.setQueryParams(date, DateTypes.DATE_FROM);
        currentWindow.start = date;
        currentWindow.end = end;
        break;
      case DateTypes.DATE_TO:
        this.setQueryParams(date, DateTypes.DATE_TO);
        currentWindow.start = start;
        currentWindow.end = date;
        break;
      default:
        console.error("Unknown type: ", type);
    }

    selected.key = "Custom";
    this.props.setTimeScale(selected);
    this.props.setTimeRange(currentWindow);
  }

  getTimeRangeTitle = () => {
    const { currentWindow, currentScale } = this.props;
    const dateFormat = "MMM DD,";
    const timeFormat = "h:mmA";
    const isSameStartDay = moment(currentWindow.start).isSame(moment(), "day");
    const isSameEndDay = moment(currentWindow.end).isSame(moment(), "day");
    if (currentScale.key === "Custom") {
      return {
        dateStart: isSameStartDay ? "" : moment.utc(currentWindow.start).format(dateFormat),
        dateEnd: isSameEndDay ? "" : moment.utc(currentWindow.end).format(dateFormat),
        timeStart: moment.utc(currentWindow.start).format(timeFormat),
        timeEnd: moment.utc(currentWindow.end).format(timeFormat),
        title: "Custom",
      };
    } else {
      return {
        title: currentScale.key,
      };
    }
  }

  generateDisabledArrows = () => {
    const { currentWindow } = this.props;
    const differenceEndToNow = moment.duration(moment().diff(currentWindow.end)).asMinutes();
    const differenceEndToStart = moment.duration(moment(currentWindow.end).diff(currentWindow.start)).asMinutes();
    const disabledArrows = [];
    if (differenceEndToNow < differenceEndToStart) {
      if (differenceEndToNow < 10) {
        disabledArrows.push(ArrowDirection.CENTER);
      }
      disabledArrows.push(ArrowDirection.RIGHT);
    }
    return disabledArrows;
  }

  render() {
    const { useTimeRange, currentScale, currentWindow } = this.props;
    return (
      <div className="timescale">
        <Divider type="vertical" />
        <Dropdown
          title={getTimeLabel(currentWindow)}
          options={[]}
          selected={currentScale.key}
          onChange={this.changeSettings}
          isTimeRange
          content={
            <RangeSelect
              value={currentWindow}
              useTimeRange={useTimeRange}
              selected={this.getTimeRangeTitle()}
              onChange={this.changeSettings}
              changeDate={this.setDate}
              options={this.getTimescaleOptions()}
            />
          }
        />
        <TimeFrameControls disabledArrows={this.generateDisabledArrows()} onArrowClick={this.arrowClick} />
      </div>
    );
  }
}

export default withRouter(connect(
  (state: AdminUIState) => {
    return {
      nodeStatusesValid: state.cachedData.nodes.valid,
      nodeStatuses: state.cachedData.nodes.data,
      currentScale: (state.timewindow as timewindow.TimeWindowState).scale,
      currentWindow: (state.timewindow as timewindow.TimeWindowState).currentWindow,
      availableScales: timewindow.availableTimeScales,
      useTimeRange: state.timewindow.useTimeRage,
      defaultTimescaleSet: timescaleDefaultSet.selector(state),
    };
  },
  {
    setTimeScale: timewindow.setTimeScale,
    setTimeRange: timewindow.setTimeRange,
    refreshNodes: refreshNodes,
    setDefaultSet: timescaleDefaultSet.set,
  },
)(TimeScaleDropdown));
