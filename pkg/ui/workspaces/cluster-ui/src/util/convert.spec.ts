// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { fromNumber } from "long";
import moment from "moment-timezone";

import {
  NanoToMilli,
  MilliToNano,
  SecondsToNano,
  TimestampToMoment,
  TimestampToNumber,
  LongToMoment,
  DurationToNumber,
  NumberToDuration,
  makeTimestamp,
} from "./convert";

const SECONDS = [0, 1, 2, 3, 4, 5, 100, 200, 300];

describe("Test convert functions", (): void => {
  describe("NanoToMilli", (): void => {
    it("should convert nanoseconds to millseconds", (): void => {
      expect(NanoToMilli(0)).toEqual(0);
      expect(NanoToMilli(1)).toEqual(1e-6);
      expect(NanoToMilli(1e6)).toEqual(1);
      expect(NanoToMilli(1e7)).toEqual(10);
    });
  });

  describe("MilliToNano", (): void => {
    it("should convert milliseconds to nanoseconds", (): void => {
      expect(MilliToNano(0)).toEqual(0);
      expect(MilliToNano(1e-6)).toEqual(1);
      expect(MilliToNano(1)).toEqual(1e6);
      expect(MilliToNano(10)).toEqual(1e7);
    });
  });

  describe("SecondsToNano", (): void => {
    it("should convert seconds to nanoseconds", (): void => {
      expect(SecondsToNano(0)).toEqual(0);
      expect(SecondsToNano(1e-9)).toEqual(1);
      expect(SecondsToNano(1)).toEqual(1e9);
      expect(SecondsToNano(10)).toEqual(1e10);
    });
  });

  describe("TimestampToMoment", (): void => {
    it("should convert a timestamp object to a moment", (): void => {
      SECONDS.forEach((seconds: number) => {
        const res = TimestampToMoment(makeTimestamp(seconds));
        const expected = moment.unix(seconds);
        expect(res.isSame(expected)).toBe(true);
      });
    });

    it("should return the provided default value if timestamp is null", (): void => {
      expect(
        TimestampToMoment(null, moment.unix(30)).isSame(moment.unix(30)),
      ).toBe(true);
    });
  });

  describe("TimestampToNumber", (): void => {
    it("should convert a timestamp object to a number", (): void => {
      SECONDS.forEach((seconds: number) => {
        const res = TimestampToNumber(makeTimestamp(seconds));
        expect(res).toEqual(seconds);
      });
    });

    it("should return the provided default value if timestamp is null", (): void => {
      expect(
        TimestampToMoment(null, moment.unix(30)).isSame(moment.unix(30)),
      ).toBe(true);
    });
  });

  describe("LongToMoment", (): void => {
    it("should convert a Long representing nanos since the epoch to a moment", (): void => {
      SECONDS.forEach((seconds: number) => {
        const res = LongToMoment(fromNumber(seconds * 1e9));
        expect(res.isSame(moment.unix(seconds))).toBe(true);
      });
    });
  });

  describe("DurationToNumber", (): void => {
    it("should convert a Duration object to the number of seconds in the duration", (): void => {
      SECONDS.forEach((seconds: number) => {
        const res = DurationToNumber(
          new protos.google.protobuf.Duration({ seconds: fromNumber(seconds) }),
        );
        expect(res).toEqual(seconds);
      });
    });

    it("should return the provided default value if Duration is null", (): void => {
      expect(DurationToNumber(null, 5)).toEqual(5);
    });
  });

  describe("NumberToDuration", (): void => {
    it("should convert a number representing seconds to a Duration", (): void => {
      SECONDS.forEach((seconds: number) => {
        const duration = NumberToDuration(seconds);
        expect(DurationToNumber(duration)).toEqual(seconds);
      });
    });

    it("should convert a number representing seconds and milliseconds to a Duration", (): void => {
      const MixedTimes = [0.005, 11.035, 1.003, 0, 0.025];
      MixedTimes.forEach((time: number) => {
        const duration = NumberToDuration(time);
        expect(DurationToNumber(duration)).toEqual(time);
      });
    });
  });
});
