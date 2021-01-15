import React from "react";
import classnames from "classnames/bind";
import styles from "./summaryCard.module.scss";

interface ISummaryCardProps {
  children: React.ReactNode;
  className?: string;
}

const cx = classnames.bind(styles);

// tslint:disable-next-line: variable-name
export const SummaryCard: React.FC<ISummaryCardProps> = ({
  children,
  className = "",
}) => <div className={`${cx("summary--card")} ${className}`}>{children}</div>;

interface ISummaryCardItemProps {
  label: React.ReactNode;
  value: React.ReactNode;
  className?: string;
}

export const SummaryCardItem: React.FC<ISummaryCardItemProps> = ({
  label,
  value,
  className = "",
}) => (
  <div className={cx("summary--card__item", className)}>
    <h4 className={cx("summary--card__item--label")}>{label}</h4>
    <p className={cx("summary--card__item--value")}>{value}</p>
  </div>
);
