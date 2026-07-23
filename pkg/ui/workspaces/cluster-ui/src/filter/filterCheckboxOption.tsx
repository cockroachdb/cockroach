// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import noop from "lodash/noop";
import React from "react";
import Select, { Props, OptionsType } from "react-select";
import { StylesConfig } from "react-select/src/styles";

import { CheckboxOption } from "../multiSelectCheckbox/multiSelectCheckbox";
import { filterLabel } from "../queryFilter/filterClasses";

export type FilterCheckboxOptionItem = { label: string; value: string };
export type FilterCheckboxOptionsType = OptionsType<FilterCheckboxOptionItem>;

export type FilterCheckboxOptionProps = {
  label: string;
  // onSelectionChanged callback function is called with all selected options.
  onSelectionChanged?: (options: OptionsType<FilterCheckboxOptionItem>) => void;
  triggerClear?: (fn: () => void) => void;
} & Props<FilterCheckboxOptionItem, true>;

export const FilterCheckboxOption = (
  props: FilterCheckboxOptionProps,
): React.ReactElement => {
  const {
    label,
    onSelectionChanged = noop,
    options,
    placeholder,
    ...selectProps
  } = props;

  const customStyles: StylesConfig<FilterCheckboxOptionItem, true> = {
    container: provided => ({
      ...provided,
      border: "none",
    }),
    option: (provided, state) => ({
      ...provided,
      backgroundColor: state.isSelected ? "#DEEBFF" : provided.backgroundColor,
      color: "#394455",
    }),
    control: provided => ({
      ...provided,
      width: "100%",
      borderColor: "#C0C6D9",
    }),
    dropdownIndicator: provided => ({
      ...provided,
      color: "#C0C6D9",
    }),
    singleValue: provided => ({
      ...provided,
      color: "#475872",
    }),
    menu: provided => ({
      ...provided,
      zIndex: 3,
    }),
  };

  return (
    <div>
      <div className={filterLabel.margin}>{label}</div>
      <Select<FilterCheckboxOptionItem, true>
        {...selectProps}
        isMulti
        options={options}
        placeholder={placeholder}
        onChange={onSelectionChanged}
        hideSelectedOptions={false}
        closeMenuOnSelect={false}
        components={{ Option: CheckboxOption }}
        styles={customStyles}
      />
    </div>
  );
};
