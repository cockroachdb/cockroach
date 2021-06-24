// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import classNames from "classnames/bind";
import { Icon, IconSize, IconType } from "./icon";
import styles from "./alignedIcon.module.scss";
const cx = classNames.bind(styles);

export interface AlignedIconProps {
  type: IconType;
  size: IconSize;
  children?: React.ReactNode;
}

export class AlignedIcon extends React.Component<AlignedIconProps> {
  render() {
    return (
      <div className={cx("root")}>
        <Icon
          type={this.props.type}
          size={this.props.size}
          className={cx(`root__icon--${this.props.size}`)}
        />
        {this.props.children}
      </div>
    );
  }
}
