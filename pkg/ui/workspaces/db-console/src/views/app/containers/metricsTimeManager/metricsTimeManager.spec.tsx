// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { shallow } from "enzyme";
import clone from "lodash/clone";
import moment from "moment-timezone";
import React from "react";

import * as timewindow from "src/redux/timeScale";

import { MetricsTimeManagerUnconnected as MetricsTimeManager } from "./";

describe("<MetricsTimeManager>", function () {
  const spy = jest.fn();
  let state: timewindow.TimeScaleState;
  const now = () => moment("11-12-1955 10:04PM -0800", "MM-DD-YYYY hh:mma Z");

  beforeEach(function () {
    state = new timewindow.TimeScaleState();
  });

  afterEach(() => {
    spy.mockReset();
  });

  const getManager = () =>
    shallow(
      <MetricsTimeManager
        timeScale={clone(state)}
        setMetricsMovingWindow={spy}
        now={now}
      />,
    );

  it("resets time window immediately it is empty", function () {
    getManager();
    expect(spy).toHaveBeenCalled();
    expect(spy.mock.calls[0][0]).toEqual({
      start: now().subtract(state.scale.windowSize),
      end: now(),
    });
  });

  it("resets time window immediately if expired", function () {
    state.metricsTime.currentWindow = {
      start: now().subtract(state.scale.windowSize),
      end: now().subtract(state.scale.windowValid).subtract(1),
    };

    getManager();
    expect(spy).toHaveBeenCalled();
    expect(spy.mock.calls[0][0]).toEqual({
      start: now().subtract(state.scale.windowSize),
      end: now(),
    });
  });

  it("resets time window immediately if scale has changed", function () {
    // valid window.
    state.metricsTime.currentWindow = {
      start: now().subtract(state.scale.windowSize),
      end: now(),
    };
    state.metricsTime.shouldUpdateMetricsWindowFromScale = true;

    getManager();
    expect(spy).toHaveBeenCalled();
    expect(spy.mock.calls[0][0]).toEqual({
      start: now().subtract(state.scale.windowSize),
      end: now(),
    });
  });

  it("resets time window later if current window is valid", function () {
    state.metricsTime.currentWindow = {
      start: now().subtract(state.scale.windowSize),
      // 5 milliseconds until expiration.
      end: now().subtract(state.scale.windowValid.asMilliseconds() - 5),
    };

    getManager();
    expect(spy).not.toHaveBeenCalled();

    // Wait 11 milliseconds, then verify that window was updated.
    return new Promise<void>((resolve, _reject) => {
      setTimeout(() => {
        expect(spy).toHaveBeenCalled();
        expect(spy.mock.calls[0][0]).toEqual({
          start: now().subtract(state.scale.windowSize),
          end: now(),
        });
        resolve();
      }, 6);
    });
  });

  // TODO (maxlang): Fix this test to actually change the state to catch the
  // issue that caused #7590. Tracked in #8595.
  it("has only a single timeout at a time.", function () {
    state.metricsTime.currentWindow = {
      start: now().subtract(state.scale.windowSize),
      // 5 milliseconds until expiration.
      end: now().subtract(state.scale.windowValid.asMilliseconds() - 5),
    };

    const manager = getManager();
    expect(spy).not.toHaveBeenCalled();

    // Set new props on currentWindow. The previous timeout should be abandoned.
    state.metricsTime.currentWindow = {
      start: now().subtract(state.scale.windowSize),
      // 10 milliseconds until expiration.
      end: now().subtract(state.scale.windowValid.asMilliseconds() - 10),
    };
    manager.setProps({
      timeWindow: state,
    });
    expect(spy).not.toHaveBeenCalled();

    // Wait 11 milliseconds, then verify that window was updated a single time.
    return new Promise<void>((resolve, _reject) => {
      setTimeout(() => {
        expect(spy).toHaveBeenCalled();
        expect(spy.mock.calls[0][0]).toEqual({
          start: now().subtract(state.scale.windowSize),
          end: now(),
        });
        resolve();
      }, 11);
    });
  });
});
