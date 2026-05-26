// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";
import React from "react";

import { Highlight } from "./highlight";
import styles from "./sqlhighlight.module.scss";

export interface SqlBoxProps {
  value: string;
  secondaryValue?: string;
}

const cx = classNames.bind(styles);

function SqlBox({ value, secondaryValue }: SqlBoxProps): React.ReactElement {
  return (
    <div className={cx("box-highlight")}>
      <Highlight value={value} secondaryValue={secondaryValue} />
    </div>
  );
}
export default SqlBox;
