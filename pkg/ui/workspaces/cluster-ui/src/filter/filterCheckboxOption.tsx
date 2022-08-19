// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import Select, { Props, OptionsType } from "react-select";
import { noop } from "lodash";
import { CheckboxOption } from "../multiSelectCheckbox/multiSelectCheckbox";
import { filterLabel } from "../queryFilter/filterClasses";
import { StylesConfig } from "react-select/src/styles";

export type FilterCheckboxOptionItem = { label: string; value: string };
export type FilterCheckboxOptionsType = OptionsType<FilterCheckboxOptionItem>;

export type FilterCheckboxOptionProps = {
  label: string;
  // onSelectionChanged callback function is called with all selected options.
  onSelectionChanged?: (options: OptionsType<FilterCheckboxOptionItem>) => void;
  triggerClear?: (fn: () => void) => void;
} & Props;

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
      <Select
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
