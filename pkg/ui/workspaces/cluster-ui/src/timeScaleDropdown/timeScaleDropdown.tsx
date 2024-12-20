// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";
import moment from "moment-timezone";
import React, { useContext, useMemo } from "react";

import { TimezoneContext } from "../contexts";
import { FormatWithTimezone, getLogger } from "../util";

import RangeSelect, {
  RangeOption,
  Selected as RangeSelectSelected,
} from "./rangeSelect";
import { TimeFrameControls } from "./timeFrameControls";
import styles from "./timeScale.module.scss";
import {
  ArrowDirection,
  TimeScale,
  TimeScaleOptions,
  TimeWindow,
} from "./timeScaleTypes";
import { defaultTimeScaleOptions, findClosestTimeScale } from "./utils";

const cx = classNames.bind(styles);

export const dateFormat = "MMM DD,";
export const timeFormat = "H:mm";

export interface TimeScaleDropdownProps {
  currentScale: TimeScale;
  options?: TimeScaleOptions;
  setTimeScale: (tw: TimeScale) => void;
  adjustTimeScaleOnChange?: (
    curTimeScale: TimeScale,
    timeWindow: TimeWindow,
  ) => TimeScale;
  hasCustomOption?: boolean;
  className?: string;
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

export const formatRangeSelectSelected = (
  currentWindow: TimeWindow,
  currentScale: TimeScale,
  timezone: string,
): RangeSelectSelected => {
  const selected = {
    timeLabel: getTimeLabel(currentWindow),
    timeWindow: currentWindow,
    key: currentScale.key,
  };

  if (currentScale.key === "Custom") {
    const start = currentWindow.start.utc();
    const end = currentWindow.end.utc();
    const endDayIsToday = moment.utc(end).isSame(moment.utc(), "day");
    const startEndOnSameDay = end.isSame(start, "day");

    const omitDayFormat = endDayIsToday && startEndOnSameDay;
    return {
      ...selected,
      dateStart: omitDayFormat ? "" : start.format(dateFormat),
      dateEnd: omitDayFormat || startEndOnSameDay ? "" : end.format(dateFormat),
      timeStart: FormatWithTimezone(start, timeFormat, timezone),
      timeEnd: FormatWithTimezone(end, timeFormat, timezone),
    };
  } else {
    return selected;
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
  hasCustomOption = true,
  className,
}): React.ReactElement => {
  const end = currentScale.fixedWindowEnd
    ? moment.utc(currentScale.fixedWindowEnd)
    : moment().utc();
  const currentWindow: TimeWindow = {
    start: moment.utc(end).subtract(currentScale.windowSize),
    end,
  };
  const timezone = useContext(TimezoneContext);

  const onPresetOptionSelect = (rangeOption: RangeOption) => {
    let timeScale: TimeScale = {
      ...options[rangeOption.label],
      key: rangeOption.label,
      fixedWindowEnd: false,
    };
    if (adjustTimeScaleOnChange) {
      const timeWindow: TimeWindow = {
        start: moment.utc().subtract(timeScale.windowSize),
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
    let key = currentScale.key;
    let endTime = moment.utc(currentWindow.end);
    // Dynamic moving window should be off unless the window extends to the current time.
    let isMoving = false;

    switch (direction) {
      case ArrowDirection.RIGHT:
        endTime = endTime.add(seconds, "seconds");
        key = "Custom";
        break;
      case ArrowDirection.LEFT:
        endTime = endTime.subtract(seconds, "seconds");
        key = "Custom";
        break;
      case ArrowDirection.CENTER:
        // Clicking `NOW` always makes the metric view become live.
        isMoving = true;

        key = Object.keys(defaultTimeScaleOptions).reduce(
          (closest, current) => {
            const currentDiff = Math.abs(
              defaultTimeScaleOptions[current].windowSize.asSeconds() -
                windowSize.asSeconds(),
            );
            const closestDiff = Math.abs(
              defaultTimeScaleOptions[closest].windowSize.asSeconds() -
                windowSize.asSeconds(),
            );
            return currentDiff < closestDiff ? current : closest;
          },
        );
        break;
      default:
        getLogger().error("Unknown direction: ", direction);
    }

    let timeScale: TimeScale;
    if (isMoving) {
      timeScale = {
        ...defaultTimeScaleOptions[key],
        key: key,
        fixedWindowEnd: false,
      };
    } else {
      timeScale = {
        ...currentScale,
        fixedWindowEnd: endTime,
        windowSize,
        key,
      };
      if (adjustTimeScaleOnChange) {
        timeScale = adjustTimeScaleOnChange(timeScale, currentWindow);
      }
    }

    setTimeScale(timeScale);
  };

  const timeScaleOptions = useMemo(() => {
    const optionsList = Object.entries(options).map(([key, value]) => ({
      value: key,
      label: key,
      timeLabel: getTimeLabel(null, value.windowSize),
    }));
    if (hasCustomOption) {
      optionsList.push({
        value: "Custom",
        label: "Custom",
        timeLabel: "--",
      });
    }
    return optionsList;
  }, [options, hasCustomOption]);

  const setDateRange = ([start, end]: [moment.Moment, moment.Moment]) => {
    const seconds = moment.duration(moment.utc(end).diff(start)).asSeconds();
    let timeScale: TimeScale = {
      ...findClosestTimeScale(options, seconds),
      windowSize: moment.duration(end.diff(start)),
      fixedWindowEnd: end,
      key: "Custom",
    };
    if (adjustTimeScaleOnChange) {
      timeScale = adjustTimeScaleOnChange(timeScale, { start, end });
    }
    setTimeScale(timeScale);
  };

  return (
    <div className={`${cx("timescale")} ${className}`}>
      <RangeSelect
        selected={formatRangeSelectSelected(
          currentWindow,
          currentScale,
          timezone,
        )}
        onPresetOptionSelect={onPresetOptionSelect}
        onCustomSelect={setDateRange}
        options={timeScaleOptions}
      />
      <TimeFrameControls
        disabledArrows={generateDisabledArrows(currentWindow)}
        onArrowClick={arrowClick}
      />
    </div>
  );
};

// getValidOption check if the option selected is valid. If is valid returns
// the selected option, otherwise  returns the first valid option.
export const getValidOption = (
  currentScale: TimeScale,
  options: TimeScaleOptions,
): TimeScale => {
  if (currentScale.key === "Custom") {
    return currentScale;
  }
  if (!(currentScale.key in options)) {
    const firstValidKey = Object.keys(options)[0];
    return {
      ...options[firstValidKey],
      key: firstValidKey,
      fixedWindowEnd: false,
    };
  }
  return currentScale;
};
