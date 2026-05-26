// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";

import styles from "./styles.module.scss";

export const commonStyles = classNames.bind(styles);

export const selectCustomStyles = {
  container: (provided: any) => ({
    ...provided,
    border: "none",
  }),
  option: (provided: any, state: any) => ({
    ...provided,
    backgroundColor: state.isSelected ? "#DEEBFF" : provided.backgroundColor,
    color: "#394455",
  }),
  control: (provided: any) => ({
    ...provided,
    width: "100%",
    borderColor: "#C0C6D9",
  }),
  dropdownIndicator: (provided: any) => ({
    ...provided,
    color: "#C0C6D9",
  }),
  singleValue: (provided: any) => ({
    ...provided,
    color: "#475872",
    fontFamily: "Lato-Regular",
    fontWeight: 400,
    fontSize: "14px",
    lineHeight: 1.5,
  }),
  indicatorSeparator: (provided: any) => ({
    ...provided,
  }),
};
