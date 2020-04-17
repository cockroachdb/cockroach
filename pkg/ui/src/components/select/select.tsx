// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import * as React from "react";
import {
  default as AntSelect,
  SelectProps as AntSelectProps,
} from "antd/es/select";
import cn from "classnames";

import "./select.styl";

interface SelectProps extends AntSelectProps {
  children: React.ReactNode;
  display?: "default" | "link";
}

export function Select(props: SelectProps) {
  const { display } = props;
  const classNames = cn("select", {
    "select--type-link": display === "link",
  });

  return (
    <AntSelect {...props} className={classNames}>
      {props.children}
    </AntSelect>
  );
}

Select.Option = AntSelect.Option;

Select.defaultProps = {
  display: "default",
};
