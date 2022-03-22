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
import { Highlight } from "./highlight";
import classNames from "classnames/bind";

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
