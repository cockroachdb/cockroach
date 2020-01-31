// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Tooltip } from "antd";
import React from "react";

interface ToolTipWrapperProps {
  text: React.ReactNode;
  short?: boolean;
  children?: React.ReactNode;
}
/**
 * ToolTipWrapper wraps its children with an area that detects mouseover events
 * and, when hovered, displays a floating tooltip to the immediate right of
 * the wrapped element.
 *
 * Note that the child element itself must be wrappable; certain CSS attributes
 * such as "float" will render parent elements unable to properly wrap their
 * contents.
 */

export class ToolTipWrapper extends React.Component<ToolTipWrapperProps, {}> {
  render() {
    const { text, children } = this.props;
    return (
      <Tooltip title={ text } placement="bottom" overlayClassName="tooltip__preset--white">
        {children}
      </Tooltip>
    );
  }
}
