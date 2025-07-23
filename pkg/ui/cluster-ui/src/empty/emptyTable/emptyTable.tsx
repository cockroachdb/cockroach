import React from "react";
import classNames from "classnames/bind";
import { isString } from "lodash";
import styles from "./emptyTable.module.scss";
import { Text, TextTypes } from "src";

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
    <Text textType={TextTypes.Heading3} className={cx("title")}>
      {title}
    </Text>
    {message && (
      <Text textType={TextTypes.Body} className={cx("message")}>
        {message}
      </Text>
    )}
    {footer && <div className={cx("footer")}>{footer}</div>}
  </div>
);
