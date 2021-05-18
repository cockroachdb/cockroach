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
import { isString } from "lodash";
import { Heading, Text } from "@cockroachlabs/ui-components";
import styles from "./emptyTable.module.scss";

export interface EmptyTableProps {
  icon?: string | React.ReactNode;
  title?: React.ReactNode;
  message?: React.ReactNode;
  footer?: React.ReactNode;
}

const cx = classNames.bind(styles);

/**
 * @description EmptyTablePlaceholder is supposed to be as a placeholder within tables when no data available.
 * It provides a brief information about the reasons why table is empty and visually shows that current state
 * is expected and valid.
 * */
export const EmptyTable: React.FC<EmptyTableProps> = ({
  icon,
  title = "No data loaded",
  message,
  footer,
}) => (
  <div className={cx("root")}>
    {icon && (
      <div className={cx("icon-container")}>
        {isString(icon) ? <img src={icon} className={cx("icon")} /> : icon}
      </div>
    )}
    <Heading type="h3">{title}</Heading>
    {message && <Text className={cx("message")}>{message}</Text>}
    {footer && <div className={cx("footer")}>{footer}</div>}
  </div>
);
