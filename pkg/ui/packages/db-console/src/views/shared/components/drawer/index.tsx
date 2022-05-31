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
import { Drawer, Button, Divider } from "antd";
import { Link } from "react-router-dom";
import classNames from "classnames/bind";
import styles from "./drawer.module.styl";

const cx = classNames.bind(styles);

interface IDrawerProps {
  visible: boolean;
  onClose: () => void;
  details?: boolean;
  children?: React.ReactNode | string;
  data: any;
}

const openDetails = (data: any) => {
  const base =
    data.app && data.app.length > 0
      ? `/statements/${data.app}/${data.implicitTxn}`
      : `/statement/${data.implicitTxn}`;
  const link = `${base}/${encodeURIComponent(data.statement)}`;
  return <Link to={link}>View statement details</Link>;
};

export const DrawerComponent = ({
  visible,
  onClose,
  children,
  data,
  details,
  ...props
}: IDrawerProps) => (
  <Drawer
    title={
      <div className={cx("__actions")}>
        <Button type="default" ghost block onClick={onClose}>
          Close
        </Button>
        {details && (
          <React.Fragment>
            <Divider type="vertical" />
            {openDetails(data)}
          </React.Fragment>
        )}
      </div>
    }
    placement="bottom"
    closable={false}
    onClose={onClose}
    visible={visible}
    className={cx("drawer--preset-black")}
    // getContainer={false}
    {...props}
  >
    {children}
  </Drawer>
);
