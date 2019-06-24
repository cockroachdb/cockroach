// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import classnames from "classnames";
import React from "react";

export interface PageConfigProps {
  layout?: "list" | "spread";
  children?: React.ReactNode;
}

export function PageConfig(props: PageConfigProps) {
  const classes = classnames({
    "page-config__list": props.layout !== "spread",
    "page-config__spread": props.layout === "spread",
  });

  return (
    <div className="page-config">
      <ul className={ classes }>
        { props.children }
      </ul>
    </div>
  );
}

export interface PageConfigItemProps {
  children?: React.ReactNode;
}

export function PageConfigItem(props: PageConfigItemProps) {
  return (
    <li className="page-config__item">
      { props.children }
    </li>
  );
}
