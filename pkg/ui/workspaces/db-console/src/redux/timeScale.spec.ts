// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { assert } from "chai";
import { defaultTimeScaleOptions } from "@cockroachlabs/cluster-ui";
import * as timeScale from "./timeScale";
import moment from "moment";

describe("time scale reducer", function() {
  describe("actions", function() {
    it("should create the correct SET_METRICS_MOVING_WINDOW action to set the current time window", function() {
      const start = moment();
      const end = start.add(10, "s");
      const expectedSetting = {
        type: timeScale.SET_METRICS_MOVING_WINDOW,
        payload: {
          start,
          end,
        },
      };
      assert.deepEqual(
        timeScale.setMetricsMovingWindow({ start, end }),
        expectedSetting,
      );
    });

    it("should create the correct SET_SCALE action to set time window settings", function() {
      const payload: timeScale.TimeScale = {
        windowSize: moment.duration(10, "s"),
        windowValid: moment.duration(10, "s"),
        sampleSize: moment.duration(10, "s"),
        fixedWindowEnd: false,
      };
      assert.deepEqual(timeScale.setTimeScale(payload), {
        type: timeScale.SET_SCALE,
        payload,
      });
    });
  });

  describe("reducer", () => {
    it("should have the correct default value.", () => {
      assert.deepEqual(
        timeScale.timeScaleReducer(undefined, { type: "unknown" }),
        new timeScale.TimeScaleState(),
      );
      assert.deepEqual(new timeScale.TimeScaleState().scale, {
        ...defaultTimeScaleOptions["Past 10 Minutes"],
        key: "Past 10 Minutes",
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
        assert.deepEqual(
          timeScale.timeScaleReducer(
            undefined,
            timeScale.setMetricsMovingWindow({ start, end }),
          ),
          expected,
        );
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
        assert.deepEqual(
          timeScale.timeScaleReducer(
            undefined,
            timeScale.setTimeScale({
              windowSize: newSize,
              windowValid: newValid,
              sampleSize: newSample,
              fixedWindowEnd: false,
            }),
          ),
          expected,
        );
      });
    });
  });
});
