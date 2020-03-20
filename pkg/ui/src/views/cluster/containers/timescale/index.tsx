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
import { queryByName, queryToObj, queryToString } from "src/util/query";
import React from "react";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { refreshNodes } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import * as timewindow from "src/redux/timewindow";
import { INodeStatus } from "src/util/proto";
import { trackTimeFrameChange } from "src/util/analytics";
import Dropdown, { ArrowDirection, DropdownOption } from "src/views/shared/components/dropdown";
import TimeFrameControls from "../../components/controls";
import RangeSelect, { DateTypes } from "../../components/range";
import "./timescale.styl";
import { Divider } from "antd";
import classNames from "classnames";

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
  useTimeRange: boolean;
}

export const getTimeLabel = (currentWindow?: timewindow.TimeWindow, windowSize?: moment.Duration) => {
  const time = windowSize ? windowSize : currentWindow ? moment.duration(moment(currentWindow.end).diff(currentWindow.start)) : moment.duration(10, "minutes");
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
  state = {
    is_opened: false,
  };

  changeSettings = (newTimescaleKey: DropdownOption) => {
    const newSettings = timewindow.availableTimeScales[newTimescaleKey.value];
    newSettings.windowEnd = null;
    if (newSettings) {
      this.setQueryParamsByDates(newSettings.windowSize, moment());
      this.props.setTimeScale({ ...newSettings, key: newTimescaleKey.value });
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
        trackTimeFrameChange("next frame");
        if (windowEnd) {
          windowEnd = windowEnd.add(seconds, "seconds");
        }
        break;
      case ArrowDirection.LEFT:
        trackTimeFrameChange("previous frame");
        windowEnd = windowEnd.subtract(seconds, "seconds");
        break;
      case ArrowDirection.CENTER:
        trackTimeFrameChange("now");
        windowEnd = moment.utc();
        break;
      default:
        console.error("Unknown direction: ", direction);
    }
    // If the timescale extends into the future then fallback to a default
    // timescale. Otherwise set the key to "Custom" so it appears correctly.
    if (!windowEnd || windowEnd > moment().subtract(currentScale.windowValid)) {
      const size = { windowSize: { _data: windowSize._data } } ;
      if (_.find(timewindow.availableTimeScales, size as any)) {
        const data = {
          ..._.find(timewindow.availableTimeScales, size as any),
          key: _.findKey(timewindow.availableTimeScales, size as any),
        };
        selected = data;
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

  componentDidMount() {
    this.props.refreshNodes();
    this.getQueryParams();
  }

  getQueryParams = () => {
    const { location } = this.props;
    const queryStart = queryByName(location, "start");
    const queryEnd = queryByName(location, "end");
    const start = queryStart && moment.unix(Number(queryStart)).utc();
    const end = queryEnd && moment.unix(Number(queryEnd)).utc();

    this.setDatesByQueryParams({ start, end });
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

  componentDidUpdate() {
    if (!this.props.nodeStatusesValid) {
      this.props.refreshNodes();
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

  setDatesByQueryParams = (dates?: timewindow.TimeWindow) => {
    const currentWindow = _.clone(this.props.currentWindow);
    const end = dates.end || currentWindow && currentWindow.end || moment();
    const start = dates.start || currentWindow && currentWindow.start || moment().subtract(10, "minutes");
    const seconds = moment.duration(moment(end).diff(start)).asSeconds();
    const timeScale = timewindow.findClosestTimeScale(seconds);
    const now = moment();
    if (moment.duration(now.diff(end)).asMinutes() > timeScale.sampleSize.asMinutes()) {
      timeScale.key = "Custom";
    }
    timeScale.windowEnd = null;
    this.props.setTimeRange({ end, start });
    this.props.setTimeScale(timeScale);
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
    if (currentScale.key === "Custom" && currentWindow) {
      const isSameStartDay = moment(currentWindow.start).isSame(moment(), "day");
      const isSameEndDay = moment(currentWindow.end).isSame(moment(), "day");
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
    const disabledArrows = [];
    if (currentWindow) {
      const differenceEndToNow = moment.duration(moment().diff(currentWindow.end)).asMinutes();
      const differenceEndToStart = moment.duration(moment(currentWindow.end).diff(currentWindow.start)).asMinutes();
      if (differenceEndToNow < differenceEndToStart) {
        if (differenceEndToNow < 10) {
          disabledArrows.push(ArrowDirection.CENTER);
        }
        disabledArrows.push(ArrowDirection.RIGHT);
      }
    }
    return disabledArrows;
  }

  onOpened = () => {
    this.setState({ is_opened: true });
  }

  onClosed = () => {
    this.setState({ is_opened: false });
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
          className={classNames({ "dropdown__focused": this.state.is_opened })}
          isTimeRange
          content={
            <RangeSelect
              onOpened={this.onOpened}
              onClosed={this.onClosed}
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
      useTimeRange: state.timewindow.useTimeRange,
    };
  },
  {
    setTimeScale: timewindow.setTimeScale,
    setTimeRange: timewindow.setTimeRange,
    refreshNodes: refreshNodes,
  },
)(TimeScaleDropdown));
