// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
import moment from "moment-timezone";
import { PayloadAction } from "src/interfaces/action";

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
        ...findClosestTimeScale(
          props.options || defaultTimeScaleOptions,
          seconds,
        ),
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

    // Only set the timescale if the url params exist.
    if (queryStart !== null && queryEnd !== null) {
      const start = queryStart && moment.unix(Number(queryStart)).utc();
      const end = queryEnd && moment.unix(Number(queryEnd)).utc();
      setDatesByQueryParams({ start, end });
    }

    // Passing an empty array of dependencies will cause this effect
    // to only run on the initial render.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const onTimeScaleChange = (timeScale: TimeScale) => {
    props.setTimeScale(timeScale);
  };

  useEffect(() => {
    // When history or props change, this effect will
    // convert the start and end of the current time scale and
    // write them to the URL as query params.
    const duration = props.currentScale.windowSize;
    const dateEnd = props.currentScale.fixedWindowEnd || moment.utc();
    const { pathname, search } = history.location;
    const urlParams = new URLSearchParams(search);
    const seconds = duration.clone().asSeconds();
    const end = dateEnd.clone();
    const start = moment.utc(end).subtract(seconds, "seconds").format("X");
    urlParams.set("start", start);
    urlParams.set("end", moment.utc(dateEnd).format("X"));
    history.push({
      pathname,
      search: urlParams.toString(),
    });
  }, [history, props]);

  return <TimeScaleDropdown {...props} setTimeScale={onTimeScaleChange} />;
};

const scaleSelector = createSelector(
  (state: AdminUIState) => state?.timeScale,
  tw => tw?.scale,
);

export default connect<
  { currentScale: TimeScale },
  { setTimeScale: (ts: TimeScale) => PayloadAction<TimeScale> },
  Partial<TimeScaleDropdownProps>
>(
  (state: AdminUIState) => ({
    currentScale: scaleSelector(state),
  }),
  {
    setTimeScale: timewindow.setTimeScale,
  },
)(TimeScaleDropdownWithSearchParams);
