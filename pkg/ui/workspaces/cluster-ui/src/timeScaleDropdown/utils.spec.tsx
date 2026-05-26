// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";

import { TimeScale } from "./timeScaleTypes";
import {
  defaultTimeScaleOptions,
  findClosestTimeScale,
  toDateRange,
  toRoundedDateRange,
} from "./utils";

describe("timescale utils", (): void => {
  describe("toDateRange", () => {
    it("get date range", () => {
      const ts: TimeScale = {
        windowSize: moment.duration(5, "day"),
        sampleSize: moment.duration(5, "minutes"),
        fixedWindowEnd: moment.utc("2022.01.10 13:42"),
        key: "Custom",
      };
      const [start, end] = toDateRange(ts);
      expect(start.format("YYYY.MM.DD HH:mm:ss")).toBe("2022.01.05 13:42:00");
      expect(end.format("YYYY.MM.DD HH:mm:ss")).toBe("2022.01.10 13:42:00");
    });
  });

  describe("toRoundedDateRange", () => {
    it("round values", () => {
      const ts: TimeScale = {
        windowSize: moment.duration(5, "day"),
        sampleSize: moment.duration(5, "minutes"),
        fixedWindowEnd: moment.utc("2022.01.10 13:42"),
        key: "Custom",
      };
      const [start, end] = toRoundedDateRange(ts);
      expect(start.format("YYYY.MM.DD HH:mm:ss")).toBe("2022.01.05 13:00:00");
      expect(end.format("YYYY.MM.DD HH:mm:ss")).toBe("2022.01.10 13:59:59");
    });

    it("already rounded values", () => {
      const ts: TimeScale = {
        windowSize: moment.duration(5, "day"),
        sampleSize: moment.duration(5, "minutes"),
        fixedWindowEnd: moment.utc("2022.01.10 13:00"),
        key: "Custom",
      };
      const [start, end] = toRoundedDateRange(ts);
      expect(start.format("YYYY.MM.DD HH:mm:ss")).toBe("2022.01.05 13:00:00");
      expect(end.format("YYYY.MM.DD HH:mm:ss")).toBe("2022.01.10 13:59:59");
    });
  });

  describe("findClosestTimeScale", () => {
    it("should find the correct time scale", () => {
      // `seconds` != window size of any of the default options, `startSeconds` not specified.
      expect(findClosestTimeScale(defaultTimeScaleOptions, 15)).toEqual({
        ...defaultTimeScaleOptions["Past 10 Minutes"],
        key: "Custom",
      });
      // `seconds` != window size of any of the default options, `startSeconds` not specified.
      expect(
        findClosestTimeScale(
          defaultTimeScaleOptions,
          moment.duration(moment().daysInMonth() * 5, "days").asSeconds(),
        ),
      ).toEqual({ ...defaultTimeScaleOptions["Past 2 Months"], key: "Custom" });
      // `seconds` == window size of one of the default options, `startSeconds` not specified.
      expect(
        findClosestTimeScale(
          defaultTimeScaleOptions,
          moment.duration(10, "minutes").asSeconds(),
        ),
      ).toEqual({
        ...defaultTimeScaleOptions["Past 10 Minutes"],
        key: "Past 10 Minutes",
      });
      // `seconds` == window size of one of the default options, `startSeconds` not specified.
      expect(
        findClosestTimeScale(
          defaultTimeScaleOptions,
          moment.duration(14, "days").asSeconds(),
        ),
      ).toEqual({
        ...defaultTimeScaleOptions["Past 2 Weeks"],
        key: "Past 2 Weeks",
      });
      // `seconds` == window size of one of the default options, `startSeconds` is now.
      expect(
        findClosestTimeScale(
          defaultTimeScaleOptions,
          defaultTimeScaleOptions["Past Hour"].windowSize.asSeconds(),
          moment().unix(),
        ),
      ).toEqual({
        ...defaultTimeScaleOptions["Past Hour"],
        key: "Past Hour",
      });
      // `seconds` == window size of one of the default options, `startSeconds` is in the past.
      expect(
        findClosestTimeScale(
          defaultTimeScaleOptions,
          defaultTimeScaleOptions["Past Hour"].windowSize.asSeconds(),
          moment().subtract(1, "day").unix(),
        ),
      ).toEqual({ ...defaultTimeScaleOptions["Past Hour"], key: "Custom" });
    });
  });
});
