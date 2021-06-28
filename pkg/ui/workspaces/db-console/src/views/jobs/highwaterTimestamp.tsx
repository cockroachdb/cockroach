// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import moment from "moment";
import { ToolTipWrapper } from "src/views/shared/components/toolTip";
import { DATE_FORMAT } from "src/util/format";
import { google } from "src/js/protos";
import ITimestamp = google.protobuf.ITimestamp;

interface HighwaterProps {
  highwater: ITimestamp;
  tooltip: string;
}

export class HighwaterTimestamp extends React.PureComponent<HighwaterProps> {
  render() {
    let highwaterMoment = moment(
      this.props.highwater.seconds.toNumber() * 1000,
    );
    // It's possible due to client clock skew that this timestamp could be in
    // the future. To avoid confusion, set a maximum bound of now.
    const now = moment();
    if (highwaterMoment.isAfter(now)) {
      highwaterMoment = now;
    }

    return (
      <ToolTipWrapper text={`System Time: ${this.props.tooltip}`}>
        High-water Timestamp: {highwaterMoment.format(DATE_FORMAT)}
      </ToolTipWrapper>
    );
  }
}
