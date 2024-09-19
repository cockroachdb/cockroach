// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";
import React from "react";

import { Highlight } from "./highlight";
import styles from "./sqlhighlight.module.styl";

export interface SqlBoxProps {
  value: string;
  secondaryValue?: string;
}

const cx = classNames.bind(styles);

class SqlBox extends React.Component<SqlBoxProps> {
  render() {
    return (
      <div className={cx("box-highlight")}>
        <Highlight {...this.props} />
      </div>
    );
  }
}
export default SqlBox;
