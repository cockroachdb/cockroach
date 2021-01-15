import classNames from "classnames/bind";
import styles from "./filter.module.scss";

const cx = classNames.bind(styles);

export const dropdownButton = cx("dropdown-btn");

export const dropdownContentWrapper = cx("dropdown-content-wrapper");

export const timePair = {
  wrapper: cx("time-pair-wrapper"),
  timeNumber: cx("time-number"),
  timeUnit: cx("time-unit"),
};

export const filterLabel = {
  app: cx("filter-label"),
  type: cx("filter-label", "filter-label__transaction-type"),
  query: cx("filter-label", "filter-label__query"),
};

export const checkbox = {
  fullScansWrapper: cx("checkbox__wrapper", "checkbox__full-scans"),
  distributedWrapper: cx("checkbox__wrapper", "checkbox__distributed"),
  label: cx("checkbox__label"),
};

export const applyBtn = {
  wrapper: cx("apply-btn__wrapper"),
  btn: cx("apply-btn__btn"),
};

export const dropdown = cx("dropdown-area");

export const hidden = cx("hide");

export const caretDown = cx("caret-down");

export const dropdownSelect = cx("dropdown-select");
