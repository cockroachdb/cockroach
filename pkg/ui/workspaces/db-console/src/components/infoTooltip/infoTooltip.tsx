// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import * as React from "react";

import { ToolTipWrapper } from "src/views/shared/components/toolTip";

import "./infoTooltip.styl";

export const InfoTooltip = (props: { text: React.ReactNode }) => {
  const { text } = props;
  return (
    <div className="info-tooltip__tooltip">
      <ToolTipWrapper text={text}>
        <div className="info-tooltip__tooltip-hover-area">
          <div className="info-tooltip__info-icon">i</div>
        </div>
      </ToolTipWrapper>
    </div>
  );
};
