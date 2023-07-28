// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Moment } from "moment-timezone";
import React, { useContext } from "react";
import { FormatWithTimezone } from "../util";
import { CoordinatedUniversalTime, TimezoneContext } from "../contexts";

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
