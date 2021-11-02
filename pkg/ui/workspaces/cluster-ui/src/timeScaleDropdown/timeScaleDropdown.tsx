// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useEffect } from "react";
import _ from "lodash";
import moment from "moment";
import classNames from "classnames/bind";
import { useLocation, useHistory } from "react-router-dom";
import {
  TimeRangeTitle,
  TimeScale,
  TimeWindow,
  ArrowDirection,
} from "./timeScaleTypes";
import TimeFrameControls from "./timeFrameControls";
import RangeSelect, { RangeOption } from "./rangeSelect";
import { Divider } from "antd";

import styles from "./timeScale.module.scss";

const cx = classNames.bind(styles);

import { availableTimeScales, findClosestTimeScale } from "./utils";

export const dateFormat = "MMM DD,";
export const timeFormat = "h:mmA";

export interface TimeScaleDropdownProps {
  currentScale: TimeScale;
  currentWindow: TimeWindow;
  setTimeScale: (tw: TimeScale) => void;
  setTimeRange: (tw: TimeWindow) => void;
}

export const getTimeLabel = (
  currentWindow?: TimeWindow,
  windowSize?: moment.Duration,
): string => {
  const time = windowSize
    ? windowSize
    : currentWindow
    ? moment.duration(moment.utc(currentWindow.end).diff(currentWindow.start))
    : moment.duration(10, "minutes");
  const seconds = time.asSeconds();
  const minutes = 60;
  const hour = minutes * 60;
  const day = hour * 24;
  const week = day * 7;
  const month = day * moment.utc().daysInMonth();
  switch (true) {
    case seconds < hour:
      return time.asMinutes().toFixed() + "m";
    case seconds >= hour && seconds < day:
      return time.asHours().toFixed() + "h";
    case seconds < week:
      return time.asDays().toFixed() + "d";
    case seconds < month:
      return time.asWeeks().toFixed() + "w";
    default:
      return time.asMonths().toFixed() + "m";
  }
};

export const getTimeRangeTitle = (
  currentWindow: TimeWindow,
  currentScale: TimeScale,
): TimeRangeTitle => {
  if (currentScale.key === "Custom" && currentWindow) {
    const isSameStartDay = moment
      .utc(currentWindow.start)
      .isSame(moment.utc(), "day");
    const isSameEndDay = moment
      .utc(currentWindow.end)
      .isSame(moment.utc(), "day");
    return {
      dateStart: isSameStartDay
        ? ""
        : moment.utc(currentWindow.start).format(dateFormat),
      dateEnd: isSameEndDay
        ? ""
        : moment.utc(currentWindow.end).format(dateFormat),
      timeStart: moment.utc(currentWindow.start).format(timeFormat),
      timeEnd: moment.utc(currentWindow.end).format(timeFormat),
      title: "Custom",
      timeLabel: getTimeLabel(currentWindow),
    };
  } else {
    return {
      title: currentScale.key,
      timeLabel: getTimeLabel(currentWindow),
    };
  }
};

export const generateDisabledArrows = (
  currentWindow: TimeWindow,
): ArrowDirection[] => {
  const disabledArrows = [];
  if (currentWindow) {
    const differenceEndToNow = moment
      .duration(moment.utc().diff(currentWindow.end))
      .asMinutes();
    const differenceEndToStart = moment
      .duration(moment.utc(currentWindow.end).diff(currentWindow.start))
      .asMinutes();
    if (differenceEndToNow < differenceEndToStart) {
      if (differenceEndToNow < 10) {
        disabledArrows.push(ArrowDirection.CENTER);
      }
      disabledArrows.push(ArrowDirection.RIGHT);
    }
  }
  return disabledArrows;
};

// TimeScaleDropdown is the dropdown that allows users to select the time range
// for graphs.
export const TimeScaleDropdown: React.FC<TimeScaleDropdownProps> = ({
  currentScale,
  currentWindow,
  setTimeScale,
  setTimeRange,
}): React.ReactElement => {
  const location = useLocation();
  const history = useHistory();

  useEffect(() => {
    const setDatesByQueryParams = (dates: Partial<TimeWindow>) => {
      const window = _.clone(currentWindow);
      const end = dates.end || (window && window.end) || moment.utc();
      const start =
        dates.start ||
        (window && window.start) ||
        moment.utc().subtract(10, "minutes");
      const seconds = moment.duration(moment.utc(end).diff(start)).asSeconds();
      const timeScale = findClosestTimeScale(seconds);
      const now = moment.utc();
      if (
        moment.duration(now.diff(end)).asMinutes() >
        timeScale.sampleSize.asMinutes()
      ) {
        timeScale.key = "Custom";
      }
      timeScale.windowEnd = null;
      setTimeRange({ end, start });
      setTimeScale(timeScale);
    };

    const urlSearchParams = new URLSearchParams(history.location.search);
    const queryStart = urlSearchParams.get("start");
    const queryEnd = urlSearchParams.get("end");
    const start = queryStart && moment.unix(Number(queryStart)).utc();
    const end = queryEnd && moment.unix(Number(queryEnd)).utc();

    setDatesByQueryParams({ start, end });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const changeSettings = (rangeOption: RangeOption) => {
    const newSettings = availableTimeScales[rangeOption.label];
    newSettings.windowEnd = null;
    if (newSettings) {
      setQueryParamsByDates(newSettings.windowSize, moment.utc());
      setTimeScale({ ...newSettings, key: rangeOption.label });
    }
  };

  const arrowClick = (direction: ArrowDirection) => {
    const windowSize: any = moment.duration(
      moment.utc(currentWindow.end).diff(currentWindow.start),
    );
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
    if (
      !windowEnd ||
      windowEnd > moment.utc().subtract(currentScale.windowValid)
    ) {
      const size = { windowSize: { _data: windowSize._data } };
      if (_.find(availableTimeScales, size as any)) {
        const data = {
          ..._.find(availableTimeScales, size as any),
          key: _.findKey(availableTimeScales, size as any),
        };
        selected = data;
      } else {
        key = "Custom";
      }
    } else {
      key = "Custom";
    }

    setQueryParamsByDates(windowSize, windowEnd);
    setTimeScale({
      ...currentScale,
      windowEnd,
      windowSize,
      key,
      ...selected,
    });
  };

  const getTimescaleOptions = () => {
    const timescaleOptions = Object.entries(availableTimeScales).map(
      ([key, value]) => {
        return {
          value: key,
          label: key,
          timeLabel: getTimeLabel(null, value.windowSize),
        };
      },
    );

    timescaleOptions.push({
      value: "Custom",
      label: "Custom",
      timeLabel: getTimeLabel(currentWindow),
    });
    return timescaleOptions;
  };

  const setQueryParamsByDates = (
    duration: moment.Duration,
    dateEnd: moment.Moment,
  ) => {
    const { pathname, search } = history.location;
    const urlParams = new URLSearchParams(search);
    const seconds = duration.clone().asSeconds();
    const end = dateEnd.clone();
    const start = moment.utc(end.subtract(seconds, "seconds")).format("X");

    urlParams.set("start", start);
    urlParams.set("end", moment.utc(dateEnd).format("X"));

    history.push({
      pathname,
      search: urlParams.toString(),
    });
  };

  const setDateRange = ([start, end]: [moment.Moment, moment.Moment]) => {
    const seconds = moment.duration(moment.utc(end).diff(start)).asSeconds();
    const timeScale = findClosestTimeScale(seconds);
    setTimeScale({
      ...timeScale,
      key: "Custom",
    });
    setTimeRange({
      ...currentWindow,
      start,
      end,
    });

    const searchParams = new URLSearchParams(location.search);
    searchParams.set("start", start.format("X"));
    searchParams.set("end", end.format("X"));
    history.replace({
      pathname: location.pathname,
      search: searchParams.toString(),
    });
  };

  return (
    <div className={cx("timescale")}>
      <Divider type="vertical" />
      <RangeSelect
        selected={getTimeRangeTitle(currentWindow, currentScale)}
        onChange={changeSettings}
        changeDate={setDateRange}
        options={getTimescaleOptions()}
      />
      <TimeFrameControls
        disabledArrows={generateDisabledArrows(currentWindow)}
        onArrowClick={arrowClick}
      />
    </div>
  );
};
