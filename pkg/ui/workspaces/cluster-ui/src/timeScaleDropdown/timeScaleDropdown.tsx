// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useMemo } from "react";
import moment from "moment";
import { Divider } from "antd";
import classNames from "classnames/bind";
import {
  TimeRangeTitle,
  TimeScale,
  TimeWindow,
  ArrowDirection,
  TimeScaleCollection,
} from "./timeScaleTypes";
import TimeFrameControls from "./timeFrameControls";
import RangeSelect, { RangeOption } from "./rangeSelect";
import { defaultTimeScaleOptions, findClosestTimeScale } from "./utils";

import styles from "./timeScale.module.scss";

const cx = classNames.bind(styles);

export const dateFormat = "MMM DD,";
export const timeFormat = "h:mmA";

export interface TimeScaleDropdownProps {
  currentScale: TimeScale;
  options?: TimeScaleCollection;
  setTimeScale: (tw: TimeScale) => void;
  adjustTimeScaleOnChange?: (
    curTimeScale: TimeScale,
    timeWindow: TimeWindow,
  ) => TimeScale;
}

export const getTimeLabel = (
  currentWindow?: TimeWindow,
  windowSize?: moment.Duration,
): string => {
  if (!currentWindow && !windowSize) return "--";
  const time = windowSize
    ? windowSize
    : moment.duration(currentWindow.end.diff(currentWindow.start));
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
  if (currentScale.key === "Custom") {
    const start = currentWindow.start.utc();
    const end = currentWindow.end.utc();
    const endDayIsToday = moment.utc(end).isSame(moment.utc(), "day");
    const startEndOnSameDay = end.isSame(start, "day");

    const omitDayFormat = endDayIsToday && startEndOnSameDay;
    return {
      dateStart: omitDayFormat ? "" : start.format(dateFormat),
      dateEnd: omitDayFormat || startEndOnSameDay ? "" : end.format(dateFormat),
      timeStart: moment.utc(start).format(timeFormat),
      timeEnd: moment.utc(end).format(timeFormat),
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
  if (!currentWindow) return [];

  const disabledArrows = [];
  const differenceEndToNow = moment
    .duration(moment.utc().diff(currentWindow.end))
    .asMinutes();
  const differenceEndToStart = moment
    .duration(moment.utc(currentWindow.end).diff(currentWindow.start))
    .asMinutes();
  if (differenceEndToNow < differenceEndToStart) {
    // Disable the "now" button if we're within 1 minute of the current time.
    if (differenceEndToNow < 1) {
      disabledArrows.push(ArrowDirection.CENTER);
    }
    disabledArrows.push(ArrowDirection.RIGHT);
  }
  return disabledArrows;
};

// TimeScaleDropdown is the dropdown that allows users to select the time range
// for the data being displayed.
export const TimeScaleDropdown: React.FC<TimeScaleDropdownProps> = ({
  currentScale,
  options = defaultTimeScaleOptions,
  setTimeScale,
  adjustTimeScaleOnChange,
}): React.ReactElement => {
  const end = currentScale.windowEnd
    ? moment.utc(currentScale.windowEnd)
    : moment().utc();
  const currentWindow: TimeWindow = {
    start: moment.utc(end).subtract(currentScale.windowSize),
    end,
  };

  const onOptionSelect = (rangeOption: RangeOption) => {
    const newSettings = options[rangeOption.label];
    newSettings.windowEnd = null;
    let timeScale: TimeScale = { ...newSettings, key: rangeOption.label };
    if (adjustTimeScaleOnChange) {
      const timeWindow: TimeWindow = {
        start: moment.utc().subtract(newSettings.windowSize),
        end: moment.utc(),
      };
      timeScale = adjustTimeScaleOnChange(timeScale, timeWindow);
    }
    setTimeScale(timeScale);
  };

  const arrowClick = (direction: ArrowDirection) => {
    const windowSize = moment.duration(
      currentWindow.end.diff(currentWindow.start),
    );

    const seconds = windowSize.asSeconds();
    let selected = {};
    let key = currentScale.key;
    let windowEnd = moment.utc(currentWindow.end);

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
        // CENTER is used to set the time window to the current time.
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
      const foundTimeScale = Object.entries(options).find(
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        ([_, value]) => value.windowSize.asSeconds() === windowSize.asSeconds(),
      );
      if (foundTimeScale) {
        selected = { key: foundTimeScale[0], ...foundTimeScale[1] };
      } else {
        key = "Custom";
      }
    } else {
      key = "Custom";
    }

    let timeScale: TimeScale = {
      ...currentScale,
      windowEnd,
      windowSize,
      key,
      ...selected,
    };
    if (adjustTimeScaleOnChange) {
      timeScale = adjustTimeScaleOnChange(timeScale, currentWindow);
    }
    setTimeScale(timeScale);
  };

  const timeScaleOptions = useMemo(() => {
    const optionsList = Object.entries(options).map(([key, value]) => ({
      value: key,
      label: key,
      timeLabel: getTimeLabel(null, value.windowSize),
    }));
    optionsList.push({
      value: "Custom",
      label: "Custom",
      timeLabel: "--",
    });
    return optionsList;
  }, [options]);

  const setDateRange = ([start, end]: [moment.Moment, moment.Moment]) => {
    const seconds = moment.duration(moment.utc(end).diff(start)).asSeconds();
    let timeScale: TimeScale = {
      ...findClosestTimeScale(options, seconds),
      windowSize: moment.duration(end.diff(start)),
      windowEnd: end,
      key: "Custom",
    };
    if (adjustTimeScaleOnChange) {
      timeScale = adjustTimeScaleOnChange(timeScale, { start, end });
    }
    setTimeScale(timeScale);
  };

  return (
    <div className={cx("timescale")}>
      <RangeSelect
        selected={getTimeRangeTitle(currentWindow, currentScale)}
        onChange={onOptionSelect}
        changeDate={setDateRange}
        options={timeScaleOptions}
      />
      <TimeFrameControls
        disabledArrows={generateDisabledArrows(currentWindow)}
        onArrowClick={arrowClick}
      />
    </div>
  );
};
