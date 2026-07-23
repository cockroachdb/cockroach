// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { render } from "@testing-library/react";
import moment from "moment-timezone";
import React from "react";
import * as reactRedux from "react-redux";

import * as timewindow from "src/redux/timeScale";

import MetricsTimeManager from "./";

// Mock react-redux hooks so the component can render without a real store.
const mockDispatch = jest.fn();
jest.mock("react-redux", () => ({
  ...jest.requireActual("react-redux"),
  useSelector: jest.fn(),
  useDispatch: () => mockDispatch,
}));

// Spy on setMetricsMovingWindow so we can verify it was called with
// the right arguments, independent of the dispatch mock.
jest.mock("src/redux/timeScale", () => {
  const actual = jest.requireActual("src/redux/timeScale");
  return {
    ...actual,
    setMetricsMovingWindow: jest.fn(actual.setMetricsMovingWindow),
  };
});

describe("<MetricsTimeManager>", () => {
  let state: timewindow.TimeScaleState;
  const now = () => moment("11-12-1955 10:04PM -0800", "MM-DD-YYYY hh:mma Z");

  beforeEach(() => {
    jest.useFakeTimers();
    state = new timewindow.TimeScaleState();
    mockDispatch.mockReset();
    (timewindow.setMetricsMovingWindow as jest.Mock).mockClear();
    (reactRedux.useSelector as jest.Mock).mockReset();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  /**
   * Set up useSelector to return the given timeScale state.
   * The component calls useSelector once per render.
   */
  function mockTimeScale(ts: timewindow.TimeScaleState) {
    (reactRedux.useSelector as jest.Mock).mockReturnValue(ts);
  }

  const renderManager = () => render(<MetricsTimeManager now={now} />);

  it("resets time window immediately when it is empty", () => {
    mockTimeScale(state);
    renderManager();
    expect(timewindow.setMetricsMovingWindow).toHaveBeenCalledWith({
      start: now().subtract(state.scale.windowSize),
      end: now(),
    });
  });

  it("resets time window immediately if expired", () => {
    state.metricsTime.currentWindow = {
      start: now().subtract(state.scale.windowSize),
      end: now().subtract(state.scale.windowValid).subtract(1),
    };

    mockTimeScale(state);
    renderManager();
    expect(timewindow.setMetricsMovingWindow).toHaveBeenCalledWith({
      start: now().subtract(state.scale.windowSize),
      end: now(),
    });
  });

  it("resets time window immediately if scale has changed", () => {
    // valid window.
    state.metricsTime.currentWindow = {
      start: now().subtract(state.scale.windowSize),
      end: now(),
    };
    state.metricsTime.shouldUpdateMetricsWindowFromScale = true;

    mockTimeScale(state);
    renderManager();
    expect(timewindow.setMetricsMovingWindow).toHaveBeenCalledWith({
      start: now().subtract(state.scale.windowSize),
      end: now(),
    });
  });

  it("resets time window later if current window is valid", () => {
    state.metricsTime.currentWindow = {
      start: now().subtract(state.scale.windowSize),
      // 5 milliseconds until expiration.
      end: now().subtract(state.scale.windowValid.asMilliseconds() - 5),
    };

    mockTimeScale(state);
    renderManager();
    expect(timewindow.setMetricsMovingWindow).not.toHaveBeenCalled();

    // Advance past expiration, then verify that window was updated.
    jest.advanceTimersByTime(6);
    expect(timewindow.setMetricsMovingWindow).toHaveBeenCalledWith({
      start: now().subtract(state.scale.windowSize),
      end: now(),
    });
  });

  it("clears timeout on unmount before it fires", () => {
    state.metricsTime.currentWindow = {
      start: now().subtract(state.scale.windowSize),
      // 5 milliseconds until expiration.
      end: now().subtract(state.scale.windowValid.asMilliseconds() - 5),
    };

    mockTimeScale(state);
    const { unmount } = renderManager();
    expect(timewindow.setMetricsMovingWindow).not.toHaveBeenCalled();

    unmount();
    jest.advanceTimersByTime(10);
    expect(timewindow.setMetricsMovingWindow).not.toHaveBeenCalled();
  });

  it("does not set timeout for fixed time ranges", () => {
    state.metricsTime.currentWindow = {
      start: now().subtract(state.scale.windowSize),
      end: now(),
    };
    state.scale.fixedWindowEnd = now();

    mockTimeScale(state);
    renderManager();
    // Fixed time ranges can't expire, so no window reset or timeout.
    expect(timewindow.setMetricsMovingWindow).not.toHaveBeenCalled();

    jest.advanceTimersByTime(10000);
    expect(timewindow.setMetricsMovingWindow).not.toHaveBeenCalled();
  });
});
