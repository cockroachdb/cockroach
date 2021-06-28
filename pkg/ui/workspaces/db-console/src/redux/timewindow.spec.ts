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
import * as timewindow from "./timewindow";
import moment from "moment";

describe("time window reducer", function () {
  describe("actions", function () {
    it("should create the correct action to set the current time window", function () {
      const start = moment();
      const end = start.add(10, "s");
      const expectedSetting = {
        type: timewindow.SET_WINDOW,
        payload: {
          start,
          end,
        },
      };
      assert.deepEqual(
        timewindow.setTimeWindow({ start, end }),
        expectedSetting,
      );
    });

    it("should create the correct action to set time window settings", function () {
      const payload: timewindow.TimeScale = {
        windowSize: moment.duration(10, "s"),
        windowValid: moment.duration(10, "s"),
        sampleSize: moment.duration(10, "s"),
      };
      assert.deepEqual(timewindow.setTimeScale(payload), {
        type: timewindow.SET_SCALE,
        payload,
      });
    });
  });

  describe("reducer", () => {
    it("should have the correct default value.", () => {
      assert.deepEqual(
        timewindow.timeWindowReducer(undefined, { type: "unknown" }),
        new timewindow.TimeWindowState(),
      );
      assert.deepEqual(
        new timewindow.TimeWindowState().scale,
        timewindow.availableTimeScales["Past 10 Minutes"],
      );
    });

    describe("setTimeWindow", () => {
      const start = moment();
      const end = start.add(10, "s");
      it("should correctly overwrite previous value", () => {
        const expected = new timewindow.TimeWindowState();
        expected.currentWindow = {
          start,
          end,
        };
        expected.scaleChanged = false;
        assert.deepEqual(
          timewindow.timeWindowReducer(
            undefined,
            timewindow.setTimeWindow({ start, end }),
          ),
          expected,
        );
      });
    });

    describe("setTimeWindowSettings", () => {
      const newSize = moment.duration(1, "h");
      const newValid = moment.duration(1, "m");
      const newSample = moment.duration(1, "m");
      it("should correctly overwrite previous value", () => {
        const expected = new timewindow.TimeWindowState();
        expected.scale = {
          windowSize: newSize,
          windowValid: newValid,
          sampleSize: newSample,
        };
        expected.scaleChanged = true;
        assert.deepEqual(
          timewindow.timeWindowReducer(
            undefined,
            timewindow.setTimeScale({
              windowSize: newSize,
              windowValid: newValid,
              sampleSize: newSample,
            }),
          ),
          expected,
        );
      });
    });
    describe("findClosestTimeScale", () => {
      it("should found correctly time scale", () => {
        assert.deepEqual(timewindow.findClosestTimeScale(15), {
          ...timewindow.availableTimeScales["Past 10 Minutes"],
          key: "Custom",
        });
        assert.deepEqual(
          timewindow.findClosestTimeScale(
            moment.duration(10, "minutes").asSeconds(),
          ),
          {
            ...timewindow.availableTimeScales["Past 10 Minutes"],
            key: "Past 10 Minutes",
          },
        );
        assert.deepEqual(
          timewindow.findClosestTimeScale(
            moment.duration(14, "days").asSeconds(),
          ),
          {
            ...timewindow.availableTimeScales["Past 2 Weeks"],
            key: "Past 2 Weeks",
          },
        );
        assert.deepEqual(
          timewindow.findClosestTimeScale(
            moment.duration(moment().daysInMonth() * 5, "days").asSeconds(),
          ),
          { ...timewindow.availableTimeScales["Past 2 Months"], key: "Custom" },
        );
      });
    });
  });
});
