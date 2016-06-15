import { assert } from "chai";
import reducer, * as timewindow from "./timewindow";
import moment = require("moment");

describe("time window reducer", function() {
  describe("actions", function() {
    it("should create the correct action to set the current time window", function() {
      let start = moment();
      let end = start.add(10, "s");
      const expectedSetting = {
        type: timewindow.SET_WINDOW,
        payload: {
          start,
          end,
        },
      };
      assert.deepEqual(
        timewindow.setTimeWindow({ start, end }),
        expectedSetting);
    });

    it("should create the correct action to set time window settings", function() {
      let windowSize = moment.duration(10, "s");
      let windowValid = moment.duration(10, "s");
      const expectedSetting = {
        type: timewindow.SET_SCALE,
        payload: {
          windowSize,
          windowValid,
        },
      };
      assert.deepEqual(
        timewindow.setTimeScale({ windowSize, windowValid }),
        expectedSetting);
    });
  });

  describe("reducer", () => {
    it("should have the correct default value.", () => {
      assert.deepEqual(
        reducer(undefined, { type: "unknown" }),
        new timewindow.TimeWindowState()
      );
      assert.deepEqual(
        (new timewindow.TimeWindowState()).scale,
        {
          windowSize: moment.duration(10, "m"),
          windowValid: moment.duration(10, "s"),
        }
      );
    });

    describe("setTimeWindow", () => {
      let start = moment();
      let end = start.add(10, "s");
      it("should correctly overwrite previous value", () => {
        let expected = new timewindow.TimeWindowState();
        expected.currentWindow = {
          start,
          end,
        };
        expected.scaleChanged = false;
        assert.deepEqual(
          reducer(undefined, timewindow.setTimeWindow({ start, end })),
          expected
        );
      });
    });

    describe("setTimeWindowSettings", () => {
      let newSize = moment.duration(1, "h");
      let newValid = moment.duration(1, "m");
      it("should correctly overwrite previous value", () => {
        let expected = new timewindow.TimeWindowState();
        expected.scale = {
          windowSize: newSize,
          windowValid: newValid,
        };
        expected.scaleChanged = true;
        assert.deepEqual(
          reducer(undefined, timewindow.setTimeScale({
            windowSize: newSize,
            windowValid: newValid,
          })),
          expected
        );
      });
    });
  });
});
