// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";
import React from "react";
import Select, { components, OptionsType } from "react-select";

import { Filter } from "../queryFilter";

import styles from "./multiSelectCheckbox.module.scss";

const cx = classNames.bind(styles);

export interface SelectOption {
  label: string;
  value: string;
  isSelected: boolean;
}

export interface MultiSelectCheckboxProps {
  field: string;
  options: SelectOption[];
  parent: Filter;
  placeholder: string;
  value?: SelectOption[];
}

/**
 * Create all options items using the values from options
 * on MultiSelectCheckbox()
 * The options must have the parameters label and isSelected
 * @param props
 * @constructor
 */
export const CheckboxOption = (props: any) => {
  return (
    <components.Option {...props}>
      <input
        type="checkbox"
        className={cx("checkbox__input")}
        checked={props.isSelected}
        onChange={() => null}
      />
      <label className={cx("checkbox__label")}>{props.label}</label>
    </components.Option>
  );
};

// customStyles uses the default styles provided from the
// react-select component and add changes
const customStyles = {
  container: (provided: any) => ({
    ...provided,
    border: "none",
    height: "fit-content",
  }),
  dropdownIndicator: (provided: any) => ({
    ...provided,
    color: "#C0C6D9",
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
  multiValue: (provided: any) => ({
    ...provided,
    backgroundColor: "#E7ECF3",
    borderRadius: "3px",
  }),
  placeholder: (provided: any) => ({
    ...provided,
    color: "#475872",
  }),
};

/**
 * Creates the MultiSelectCheckbox from the props
 * parent (any): the element creating this multiselect that will have its state
 * updated when a new value is selected
 * field (string): the name of the state's field on the parent that will be
 * updated
 * options (SelectOption[]): a list of options. Each option object must contain a
 * label, value and isSelected parameters
 * placeholder (string): the placeholder for the multiselect
 * value (SelectOption[]): a list of the selected options (optional)
 * @constructor
 * @param props
 */
export const MultiSelectCheckbox = (props: MultiSelectCheckboxProps) => {
  const handleChange = (
    selectedOptions: OptionsType<SelectOption>,
    field: string,
    parent: Filter,
  ) => {
    const selected =
      selectedOptions
        ?.map(function (option: SelectOption) {
          return option.label;
        })
        .toString() || "";
    parent.setState({
      filters: {
        ...parent.state.filters,
        [field]: selected,
      },
    });
  };

  return (
    <Select
      isMulti
      options={props.options}
      placeholder={props.placeholder}
      value={props.value}
      onChange={selected => handleChange(selected, props.field, props.parent)}
      hideSelectedOptions={false}
      closeMenuOnSelect={false}
      components={{ Option: CheckboxOption }}
      styles={customStyles}
    />
  );
};
