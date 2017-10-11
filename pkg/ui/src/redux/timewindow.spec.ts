import { assert } from "chai";
import * as timewindow from "./timewindow";
import moment from "moment";

describe("time window reducer", function() {
  describe("actions", function() {
    it("should create the correct action to set the current time window", function() {
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
        expectedSetting);
    });

    it("should create the correct action to set time window settings", function() {
      const payload: timewindow.TimeScale = {
        windowSize: moment.duration(10, "s"),
        windowValid: moment.duration(10, "s"),
        sampleSize: moment.duration(10, "s"),
      };
      assert.deepEqual(
        timewindow.setTimeScale(payload),
        { type: timewindow.SET_SCALE, payload },
      );
    });
  });

  describe("reducer", () => {
    it("should have the correct default value.", () => {
      assert.deepEqual(
        timewindow.timeWindowReducer(undefined, { type: "unknown" }),
        new timewindow.TimeWindowState(),
      );
      assert.deepEqual(
        (new timewindow.TimeWindowState()).scale,
        timewindow.availableTimeScales["10 min"],
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
          timewindow.timeWindowReducer(undefined, timewindow.setTimeWindow({ start, end })),
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
          timewindow.timeWindowReducer(undefined, timewindow.setTimeScale({
            windowSize: newSize,
            windowValid: newValid,
            sampleSize: newSample,
          })),
          expected,
        );
      });
    });
  });
});
