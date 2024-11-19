// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { defaultTimeScaleOptions, TimeScale } from "@cockroachlabs/cluster-ui";
import moment from "moment-timezone";

import * as timeScale from "./timeScale";

describe("time scale reducer", function () {
  describe("actions", function () {
    it("should create the correct SET_METRICS_MOVING_WINDOW action to set the current time window", function () {
      const start = moment();
      const end = start.add(10, "s");
      const expectedSetting = {
        type: timeScale.SET_METRICS_MOVING_WINDOW,
        payload: {
          start,
          end,
        },
      };
      expect(timeScale.setMetricsMovingWindow({ start, end })).toEqual(
        expectedSetting,
      );
    });

    it("should create the correct SET_SCALE action to set time window settings", function () {
      const payload: TimeScale = {
        windowSize: moment.duration(10, "s"),
        windowValid: moment.duration(10, "s"),
        sampleSize: moment.duration(10, "s"),
        fixedWindowEnd: false,
      };
      expect(timeScale.setTimeScale(payload)).toEqual({
        type: timeScale.SET_SCALE,
        payload,
      });
    });
  });

  describe("reducer", () => {
    it("should have the correct default value.", () => {
      expect(
        timeScale.timeScaleReducer(undefined, { type: "unknown" }),
      ).toEqual(new timeScale.TimeScaleState());
      expect(new timeScale.TimeScaleState().scale).toEqual({
        ...defaultTimeScaleOptions["Past Hour"],
        key: "Past Hour",
        fixedWindowEnd: false,
      });
    });

    describe("setMetricsMovingWindow", () => {
      const start = moment();
      const end = start.add(10, "s");
      it("should correctly overwrite previous value", () => {
        const expected = new timeScale.TimeScaleState();
        expected.metricsTime.currentWindow = {
          start,
          end,
        };
        expected.metricsTime.shouldUpdateMetricsWindowFromScale = false;
        expect(
          timeScale.timeScaleReducer(
            undefined,
            timeScale.setMetricsMovingWindow({ start, end }),
          ),
        ).toEqual(expected);
      });
    });

    describe("setTimeScale", () => {
      const newSize = moment.duration(1, "h");
      const newValid = moment.duration(1, "m");
      const newSample = moment.duration(1, "m");
      it("should correctly overwrite previous value", () => {
        const expected = new timeScale.TimeScaleState();
        expected.scale = {
          windowSize: newSize,
          windowValid: newValid,
          sampleSize: newSample,
          fixedWindowEnd: false,
        };
        expected.metricsTime.shouldUpdateMetricsWindowFromScale = true;
        expect(
          timeScale.timeScaleReducer(
            undefined,
            timeScale.setTimeScale({
              windowSize: newSize,
              windowValid: newValid,
              sampleSize: newSample,
              fixedWindowEnd: false,
            }),
          ),
        ).toEqual(expected);
      });
    });
  });
});
