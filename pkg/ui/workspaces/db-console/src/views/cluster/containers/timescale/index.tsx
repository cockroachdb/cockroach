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
import React, { useEffect, useState } from "react";
import { connect } from "react-redux";
import { useLocation, useHistory } from "react-router-dom";
import { refreshNodes } from "src/redux/apiReducers";
import { AdminUIState } from "src/redux/state";
import * as timewindow from "src/redux/timewindow";
import { trackTimeFrameChange } from "src/util/analytics";
import Dropdown, {
  ArrowDirection,
  DropdownOption,
} from "src/views/shared/components/dropdown";
import TimeFrameControls from "../../components/controls";
import RangeSelect from "../../components/range";
import "./timescale.styl";
import { Divider } from "antd";
import classNames from "classnames";
import { createSelector } from "reselect";

export const dateFormat = "MMM DD,";
export const timeFormat = "h:mmA";

export interface TimeScaleDropdownProps {
  currentScale: timewindow.TimeScale;
  currentWindow: timewindow.TimeWindow;
  setTimeScale: typeof timewindow.setTimeScale;
  setTimeRange: typeof timewindow.setTimeRange;
  // Track node data to find the oldest node and set the default timescale.
  dispatchRefreshNodes: typeof refreshNodes;
  nodeStatusesValid: boolean;
  // Track whether the default has been set.
  useTimeRange: boolean;
}

export const getTimeLabel = (
  currentWindow?: timewindow.TimeWindow,
  windowSize?: moment.Duration,
) => {
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
  currentWindow: timewindow.TimeWindow,
  currentScale: timewindow.TimeScale,
) => {
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
    };
  } else {
    return {
      title: currentScale.key,
    };
  }
};

export const generateDisabledArrows = (
  currentWindow: timewindow.TimeWindow,
) => {
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
  dispatchRefreshNodes,
  nodeStatusesValid,
  useTimeRange,
}) => {
  const location = useLocation();
  const history = useHistory();
  useEffect(() => {
    if (!nodeStatusesValid) {
      dispatchRefreshNodes();
    }
  });
  useEffect(() => {
    getQueryParams();
    // We want this to just run once like a componentDidMount function but we
    // still need an empty deps array, can't remove it.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const [isOpened, setIsOpened] = useState(false);

  const changeSettings = (newTimescaleKey: DropdownOption) => {
    const newSettings = timewindow.availableTimeScales[newTimescaleKey.value];
    newSettings.windowEnd = null;
    if (newSettings) {
      setQueryParamsByDates(newSettings.windowSize, moment.utc());
      setTimeScale({ ...newSettings, key: newTimescaleKey.value });
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
    if (
      !windowEnd ||
      windowEnd > moment.utc().subtract(currentScale.windowValid)
    ) {
      const size = { windowSize: { _data: windowSize._data } };
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
    const timescaleOptions = _.map(timewindow.availableTimeScales, (_ts, k) => {
      return {
        value: k,
        label: k,
        timeLabel: getTimeLabel(null, _ts.windowSize),
      };
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
  };

  const getQueryParams = () => {
    const urlSearchParams = new URLSearchParams(history.location.search);
    const queryStart = urlSearchParams.get("start");
    const queryEnd = urlSearchParams.get("end");
    const start = queryStart && moment.unix(Number(queryStart)).utc();
    const end = queryEnd && moment.unix(Number(queryEnd)).utc();

    setDatesByQueryParams({ start, end });
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

  const setDatesByQueryParams = (dates?: timewindow.TimeWindow) => {
    const window = _.clone(currentWindow);
    const end = dates.end || (window && window.end) || moment.utc();
    const start =
      dates.start ||
      (window && window.start) ||
      moment.utc().subtract(10, "minutes");
    const seconds = moment.duration(moment.utc(end).diff(start)).asSeconds();
    const timeScale = timewindow.findClosestTimeScale(seconds);
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

  const setDateRange = ([start, end]: [moment.Moment, moment.Moment]) => {
    const seconds = moment.duration(moment.utc(end).diff(start)).asSeconds();
    const timeScale = timewindow.findClosestTimeScale(seconds);
    setTimeScale({
      ...timeScale,
      key: "Custom",
    });
    setTimeRange({
      ...window,
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

  const onOpened = () => {
    setIsOpened(true);
  };

  const onClosed = () => {
    setIsOpened(false);
  };

  return (
    <div className="timescale">
      <Divider type="vertical" />
      <Dropdown
        title={getTimeLabel(currentWindow)}
        options={[]}
        selected={currentScale.key}
        onChange={changeSettings}
        className={classNames({ dropdown__focused: isOpened })}
        isTimeRange
        content={
          <RangeSelect
            onOpened={onOpened}
            onClosed={onClosed}
            value={currentWindow}
            useTimeRange={useTimeRange}
            selected={getTimeRangeTitle(currentWindow, currentScale)}
            onChange={changeSettings}
            changeDate={setDateRange}
            options={getTimescaleOptions()}
          />
        }
      />
      <TimeFrameControls
        disabledArrows={generateDisabledArrows(currentWindow)}
        onArrowClick={arrowClick}
      />
    </div>
  );
};

const scaleSelector = createSelector(
  (state: AdminUIState) => state?.timewindow,
  (tw) => tw?.scale,
);

const currentWindowSelector = createSelector(
  (state: AdminUIState) => state?.timewindow,
  (tw) => tw?.currentWindow,
);

const timeRangeSelector = createSelector(
  (state: AdminUIState) => state?.timewindow,
  (tw) => tw?.useTimeRange,
);

const isValidNodeStatus = createSelector(
  (state: AdminUIState) => state?.cachedData?.nodes,
  (nodes) => nodes?.valid,
);

export default connect(
  (state: AdminUIState) => ({
    nodeStatusesValid: isValidNodeStatus(state),
    currentScale: scaleSelector(state),
    currentWindow: currentWindowSelector(state),
    useTimeRange: timeRangeSelector(state),
  }),
  {
    setTimeScale: timewindow.setTimeScale,
    setTimeRange: timewindow.setTimeRange,
    dispatchRefreshNodes: refreshNodes,
  },
)(TimeScaleDropdown);
