// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useEffect } from "react";
import { useHistory } from "react-router-dom";
import { connect } from "react-redux";
import { AdminUIState } from "src/redux/state";
import * as timewindow from "src/redux/timewindow";
import {
  defaultTimeScaleOptions,
  TimeScaleDropdown,
  TimeScaleDropdownProps,
  TimeScale,
  TimeWindow,
  findClosestTimeScale,
} from "@cockroachlabs/cluster-ui";
import { createSelector } from "reselect";
import moment from "moment";

// The time scale dropdown from cluster-ui that updates route params as
// options are selected.
const TimeScaleDropdownWithSearchParams = (
  props: TimeScaleDropdownProps,
): React.ReactElement => {
  const history = useHistory();

  useEffect(() => {
    const setDatesByQueryParams = (dates: Partial<TimeWindow>) => {
      const now = moment.utc();
      const currentWindow: TimeWindow = {
        start: moment(now).subtract(props.currentScale.windowSize),
        end: now,
      };
      const end = dates.end?.utc() || currentWindow.end?.utc() || now;
      const start =
        dates.start?.utc() ||
        currentWindow.start?.utc() ||
        moment(now).subtract(10, "minutes");
      const seconds = end.diff(start, "seconds");
      const timeScale = findClosestTimeScale(defaultTimeScaleOptions, seconds);
      if (
        moment.duration(now.diff(end)).asMinutes() >
        timeScale.sampleSize.asMinutes()
      ) {
        timeScale.key = "Custom";
      }
      timeScale.windowEnd = null;
      timeScale.windowSize = moment.duration(end.diff(start));
      props.setTimeScale(timeScale);
    };

    const urlSearchParams = new URLSearchParams(history.location.search);
    const queryStart = urlSearchParams.get("start");
    const queryEnd = urlSearchParams.get("end");
    const start = queryStart && moment.unix(Number(queryStart)).utc();
    const end = queryEnd && moment.unix(Number(queryEnd)).utc();

    setDatesByQueryParams({ start, end });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const setQueryParamsByDates = (
    duration: moment.Duration,
    dateEnd: moment.Moment,
  ) => {
    const { pathname, search } = history.location;
    const urlParams = new URLSearchParams(search);
    const seconds = duration.clone().asSeconds();
    const end = dateEnd.clone();
    const start = moment
      .utc(end)
      .subtract(seconds, "seconds")
      .format("X");

    urlParams.set("start", start);
    urlParams.set("end", moment.utc(dateEnd).format("X"));

    history.push({
      pathname,
      search: urlParams.toString(),
    });
  };

  const onTimeScaleChange = (timeScale: TimeScale) => {
    props.setTimeScale(timeScale);
    setQueryParamsByDates(
      timeScale.windowSize,
      timeScale.windowEnd || moment.utc(),
    );
  };

  return <TimeScaleDropdown {...props} setTimeScale={onTimeScaleChange} />;
};

const scaleSelector = createSelector(
  (state: AdminUIState) => state?.timewindow,
  tw => tw?.scale,
);

export default connect(
  (state: AdminUIState) => ({
    currentScale: scaleSelector(state),
  }),
  {
    setTimeScale: timewindow.setTimeScale,
  },
)(TimeScaleDropdownWithSearchParams);
