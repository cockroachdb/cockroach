// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Moment } from "moment-timezone";
import React, { useContext } from "react";

import { CoordinatedUniversalTime, TimezoneContext } from "../contexts";
import { FormatWithTimezone } from "../util";

export function Timezone() {
  const timezone = useContext(TimezoneContext);
  return (
    <>
      {timezone.toLowerCase() === CoordinatedUniversalTime.toLowerCase()
        ? "(UTC)" // People prefer to read "UTC" instead of the IANA standard "etc/UTC".
        : `(${timezone})`}
    </>
  );
}

export function Timestamp(props: {
  time?: Moment;
  format: string;
  fallback?: string;
}) {
  const timezone = useContext(TimezoneContext);
  const { time, format, fallback } = props;
  return time ? (
    <>{FormatWithTimezone(time, format, timezone)}</>
  ) : (
    <>{fallback}</>
  );
}
