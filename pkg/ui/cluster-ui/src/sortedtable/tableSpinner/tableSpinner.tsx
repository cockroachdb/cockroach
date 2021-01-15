import React from "react";
import { Spinner } from "@cockroachlabs/icons";
import { Spin, Icon } from "antd";
import classNames from "classnames/bind";
import styles from "./tableSpinner.module.scss";

const cx = classNames.bind(styles);

interface TableSpinnerProps {
  loadingLabel: string;
}

export const TableSpinner = ({ loadingLabel }: TableSpinnerProps) => {
  const tableSpinnerClass = cx("table__loading");
  const spinClass = cx("table__loading--spin");
  const loadingLabelClass = cx("table__loading--label");

  return (
    <div className={tableSpinnerClass}>
      <Spin
        className={spinClass}
        indicator={<Icon component={Spinner} spin />}
      />
      {loadingLabel && (
        <span className={loadingLabelClass}>{loadingLabel}</span>
      )}
    </div>
  );
};
