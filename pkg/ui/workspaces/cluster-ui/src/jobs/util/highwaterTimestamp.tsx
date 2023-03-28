// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import { google } from "@cockroachlabs/crdb-protobuf-client";
import { Tooltip } from "@cockroachlabs/ui-components";
import moment from "moment-timezone";
import React from "react";
import { DATE_FORMAT_24_TZ } from "src/util/format";
import { Timestamp } from "../../timestamp";

type ITimestamp = google.protobuf.ITimestamp;

interface HighwaterProps {
  timestamp: ITimestamp;
  decimalString: string;
}

export class HighwaterTimestamp extends React.PureComponent<HighwaterProps> {
  render(): React.ReactElement {
    if (!this.props.timestamp) {
      return null;
    }
    let highwaterMoment = moment(
      this.props.timestamp.seconds.toNumber() * 1000,
    );
    // It's possible due to client clock skew that this timestamp could be in
    // the future. To avoid confusion, set a maximum bound of now.
    const now = moment();
    if (highwaterMoment.isAfter(now)) {
      highwaterMoment = now;
    }

    return (
      <Tooltip
        placement="bottom"
        style="default"
        content={
          <Timestamp time={highwaterMoment} format={DATE_FORMAT_24_TZ} />
        }
      >
        {this.props.decimalString}
      </Tooltip>
    );
  }
}
