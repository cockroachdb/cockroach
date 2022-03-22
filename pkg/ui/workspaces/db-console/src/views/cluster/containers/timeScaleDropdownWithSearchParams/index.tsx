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
import * as timewindow from "src/redux/timeScale";
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
      // `currentWindow` is derived from `scale`, and does not have to do with the `currentWindow` for the metrics page.
      const currentWindow: TimeWindow = {
        start: moment(now).subtract(props.currentScale.windowSize),
        end: now,
      };
      /**
       * Prioritize an end defined in the query params.
       * Else, use window end.
       * Else, a seemingly unreachable option says otherwise use now, but that should never happen since it is set in
       *  the line above (and is the same value anyway, always now).
       */
      const end = dates.end?.utc() || currentWindow.end?.utc() || now;
      /**
       * Prioritize start as defined in the query params.
       * Else, use now minus the window size.
       * Else, a final seemingly unreachable option (since start is always set above) is to do ten minutes before now.
       */
      const start =
        dates.start?.utc() ||
        currentWindow.start?.utc() ||
        moment(now).subtract(10, "minutes");
      const seconds = end.diff(start, "seconds");

      // Find the closest time scale just by window size.
      // And temporarily assume the end is "now" with fixedWindowEnd=false.
      const timeScale: TimeScale = {
        ...findClosestTimeScale(defaultTimeScaleOptions, seconds),
        windowSize: moment.duration(end.diff(start)),
        fixedWindowEnd: false,
      };
      // Check if the end is close to now, with "close" defined as being no more than `sampleSize` behind.
      if (now > end.subtract(timeScale.sampleSize)) {
        // The end is far enough away from now, thus this is a custom selection.
        timeScale.key = "Custom";
        timeScale.fixedWindowEnd = end;
      }
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
      timeScale.fixedWindowEnd || moment.utc(),
    );
  };

  return <TimeScaleDropdown {...props} setTimeScale={onTimeScaleChange} />;
};

const scaleSelector = createSelector(
  (state: AdminUIState) => state?.timeScale,
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
