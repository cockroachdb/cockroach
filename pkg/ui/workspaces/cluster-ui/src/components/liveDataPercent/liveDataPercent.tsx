// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Row } from "antd";
import React from "react";

import { Bytes, Percentage } from "src/util";

type Props = {
  // Float between 0-1.
  liveBytes: number;
  totalBytes: number;
};

export const LiveDataPercent: React.FC<Props> = ({ liveBytes, totalBytes }) => {
  return (
    <div>
      <Row justify={"end"}>
        {totalBytes ? Percentage(liveBytes, totalBytes, 1) : "0.0%"}
      </Row>
      <Row justify={"end"}>
        {Bytes(liveBytes)} live data / {Bytes(totalBytes)} total
      </Row>
    </div>
  );
};
