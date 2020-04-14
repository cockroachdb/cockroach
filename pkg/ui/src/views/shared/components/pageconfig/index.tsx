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
import styles from "./pageConfig.module.styl";

export interface PageConfigProps {
  layout?: "list" | "spread";
  children?: React.ReactNode;
}

export function PageConfig(props: PageConfigProps) {
  const classes = classnames({
    [styles.pageConfig__list]: props.layout !== "spread",
    [styles.pageConfig__spread]: props.layout === "spread",
  });

  console.log(styles)

  return (
    <div className={styles.pageConfig}>
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
    <li className={styles.pageConfig__item}>
      { props.children }
    </li>
  );
}
