// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
