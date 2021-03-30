import classNames from "classnames/bind";
import styles from "./filter.module.scss";

const cx = classNames.bind(styles);

export const dropdownButton = cx("dropdown-btn");
export const dropdownContentWrapper = cx("dropdown-content-wrapper");
export const dropdown = cx("dropdown-area");
export const hidden = cx("hide");
export const caretDown = cx("caret-down");
export const dropdownSelect = cx("dropdown-select");

export const timePair = {
  wrapper: cx("time-pair-wrapper"),
  timeNumber: cx("time-number"),
  timeUnit: cx("time-unit"),
};

export const filterLabel = {
  top: cx("filter-label"),
  margin: cx("filter-label", "filter-label__margin-top"),
};

export const checkbox = {
  input: cx("checkbox__input"),
  label: cx("checkbox__label"),
};

export const applyBtn = {
  wrapper: cx("apply-btn__wrapper"),
  btn: cx("apply-btn__btn"),
};
