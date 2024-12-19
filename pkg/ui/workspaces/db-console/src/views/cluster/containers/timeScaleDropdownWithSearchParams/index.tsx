// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  defaultTimeScaleOptions,
  TimeScaleDropdown,
  TimeScaleDropdownProps,
  TimeScale,
  TimeWindow,
  findClosestTimeScale,
} from "@cockroachlabs/cluster-ui";
import moment from "moment-timezone";
import React, { useEffect, useState } from "react";
import { connect } from "react-redux";
import { useHistory } from "react-router-dom";
import { createSelector } from "reselect";

import { PayloadAction } from "src/interfaces/action";
import { AdminUIState } from "src/redux/state";
import * as timewindow from "src/redux/timeScale";

// toQueryParamTimescale converts a preset name to a query param
// format, replacing spaces with dashes.
function toQueryParamTimescale(str: string): string {
  return str.toLowerCase().replace(/\s+/g, "-");
}

const findPreset = (s: string): string | undefined => {
  return Object.keys(defaultTimeScaleOptions).find(key => {
    if (toQueryParamTimescale(key) === s) {
      return defaultTimeScaleOptions[key];
    }
  });
};

// The time scale dropdown from cluster-ui that updates route params as
// options are selected.
// The query params formats we support are the following:
//   `?start=unix_timestamp&end=unix_timestamp`
//   `?preset=string`
//
// When the component is loaded, we will inspect the query params and
// populate the time scale dropdown. If the user manipulates the
// component and modifies items, the query params will be sync-ed.
const TimeScaleDropdownWithSearchParams = (
  props: TimeScaleDropdownProps,
): React.ReactElement => {
  const history = useHistory();

  // `queryParamsRead` tracks whether this component has synced state
  // from the location bar's query params. We do this on initial mount
  // just once. Once done, we make sure that future redux store updates
  // get pushed to the query params. This prevents confusion about
  // bi-directional updates. Any component outside of this one can just
  // update redux state and the query params will sync.
  const [queryParamsRead, setQueryParamsRead] = useState(false);

  useEffect(() => {
    if (queryParamsRead) {
      return;
    }
    const setDatesByQueryParams = (
      dates?: Partial<TimeWindow>,
      preset?: string,
    ) => {
      if (preset) {
        const presetTimescale = findPreset(preset);
        if (!presetTimescale) {
          return;
        }
        const presetOption = defaultTimeScaleOptions[presetTimescale];

        props.setTimeScale({
          ...presetOption,
          key: presetTimescale,
          fixedWindowEnd: false,
        });
        return;
      }

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
    const preset = urlSearchParams.get("preset");

    if (queryStart && queryEnd) {
      const start = moment.unix(Number(queryStart)).utc();
      const end = moment.unix(Number(queryEnd)).utc();
      setDatesByQueryParams({ start, end });
    } else if (preset) {
      setDatesByQueryParams({}, preset);
    } else {
      // Set query params from the redux store if there aren't any
      // present in the URL.
      onTimeScaleChange(props.currentScale);
    }

    setQueryParamsRead(true);
    // Passing an empty array of dependencies will cause this effect
    // to only run on the initial render.
    /* eslint react-hooks/exhaustive-deps: "off" */
  }, []);

  // This will get triggered if the redux store updates the
  // currentScale, we sync the query params to match.
  useEffect(() => {
    if (queryParamsRead) {
      onTimeScaleChange(props.currentScale);
    }
  }, [props.currentScale]);

  const onTimeScaleChange = (timeScale: TimeScale) => {
    if (!timeScale.fixedWindowEnd) {
      const preset = findClosestTimeScale(
        defaultTimeScaleOptions,
        timeScale.windowSize.asSeconds(),
      ).key;
      const { pathname, search } = history.location;
      const urlParams = new URLSearchParams(search);
      urlParams.set("preset", toQueryParamTimescale(preset));
      urlParams.delete("start");
      urlParams.delete("end");
      history.push({
        pathname,
        search: urlParams.toString(),
      });
    } else {
      const duration = timeScale.windowSize;
      const dateEnd = timeScale.fixedWindowEnd || moment.utc();
      const { pathname, search } = history.location;
      const urlParams = new URLSearchParams(search);
      const seconds = duration.clone().asSeconds();
      const end = dateEnd.clone();
      const start = moment.utc(end).subtract(seconds, "seconds").format("X");
      urlParams.set("start", start);
      urlParams.set("end", moment.utc(dateEnd).format("X"));
      urlParams.delete("preset");
      history.push({
        pathname,
        search: urlParams.toString(),
      });
    }

    // Pushes changes to the session storage.
    props.setTimeScale(timeScale);
  };

  return <TimeScaleDropdown {...props} setTimeScale={onTimeScaleChange} />;
};

const scaleSelector = createSelector(
  (state: AdminUIState) => state?.timeScale,
  tw => tw?.scale,
);

type MapStateToProps = {
  currentScale: TimeScale;
};

type MapDispatchToProps = {
  setTimeScale: (ts: TimeScale) => PayloadAction<TimeScale>;
};

export default connect<
  MapStateToProps,
  MapDispatchToProps,
  Partial<TimeScaleDropdownProps>,
  AdminUIState
>(
  (state: AdminUIState): MapStateToProps => ({
    currentScale: scaleSelector(state),
  }),
  {
    setTimeScale: timewindow.setTimeScale,
  },
)(TimeScaleDropdownWithSearchParams);
