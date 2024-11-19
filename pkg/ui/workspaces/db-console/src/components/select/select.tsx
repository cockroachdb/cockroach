// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  default as AntSelect,
  SelectProps as AntSelectProps,
} from "antd/es/select";
import cn from "classnames";
import * as React from "react";

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
