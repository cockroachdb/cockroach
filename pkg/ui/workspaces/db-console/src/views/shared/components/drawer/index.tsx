// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Drawer, Button, Divider } from "antd";
import classNames from "classnames/bind";
import React from "react";
import { Link } from "react-router-dom";

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
    open={visible}
    rootClassName={cx("drawer--preset-black")}
    {...props}
  >
    {children}
  </Drawer>
);
