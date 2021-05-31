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
import Select, { components, OptionsType } from "react-select";
import styles from "./multiSelectCheckbox.module.scss";
import classNames from "classnames/bind";

const cx = classNames.bind(styles);

export interface SelectOption {
  label: string;
  value: string;
  isSelected: boolean;
}

export interface MultiSelectCheckboxProps {
  field: string;
  options: SelectOption[];
  parent: any;
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
const CheckboxOption = (props: any) => {
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
  option: (provided: any, state: any) => ({
    ...provided,
    backgroundColor: state.isSelected ? "#DEEBFF" : provided.backgroundColor,
    color: "#394455",
  }),
  control: (provided: any) => ({
    ...provided,
    width: "100%",
  }),
  multiValue: (provided: any) => ({
    ...provided,
    backgroundColor: "#E7ECF3",
    borderRadius: "3px",
  }),
};

/**
 * Creates the MultiSelectCheckbox from the props
 * @param props:
 * parent (any): the element creating this multiselect that will have its state
 * updated when a new value is selected
 * field (string): the name of the state's field on the parent that will be
 * updated
 * options (SelectOption[]): a list of options. Each option object must contain a
 * label, value and isSelected parameters
 * placeholder (string): the placeholder for the multiselect
 * value (SelectOption[]): a list of the selected options (optional)
 * @constructor
 */
export const MultiSelectCheckbox = (props: MultiSelectCheckboxProps) => {
  const handleChange = (
    selectedOptions: OptionsType<SelectOption>,
    field: string,
    parent: any,
  ) => {
    const selected = selectedOptions
      .map(function(option: SelectOption) {
        return option.label;
      })
      .toString();
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
