// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { Moment } from "moment";
import { Text, TextTypes } from "src/components";

export interface DateRangeLabelProps {
  from: Moment;
  to: Moment;
}

export const DateRangeLabel: React.FC<DateRangeLabelProps> = ({ from, to }) => {
  const dateFormat = "MMM D";
  const timeFormat = "LT";
  const fromDateStr = from.format(dateFormat);
  const toDateStr = to.format(dateFormat);
  const fromTimeStr = from.format(timeFormat);
  const toTimeStr = to.format(timeFormat);
  const isUTC = to.isUTC() && from.isUTC();
  return (
    <div style={{ textAlign: "left" }}>
      <Text textType={TextTypes.Body}>
        {fromDateStr}
        {", "}
      </Text>
      <Text textType={TextTypes.BodyStrong}>{fromTimeStr}</Text>
      <Text textType={TextTypes.Body}>{" — "}</Text>
      <Text textType={TextTypes.Body}>
        {toDateStr}
        {", "}
      </Text>
      <Text textType={TextTypes.BodyStrong}>{toTimeStr}</Text>
      {isUTC && <Text textType={TextTypes.Body}>{" UTC"}</Text>}
    </div>
  );
};
