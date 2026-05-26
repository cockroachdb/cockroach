// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";

import { Dropdown, DropdownOption } from "src/dropdown";

export type DropDownSelectProps = {
  selectedLabel: string;
  onViewChange: (updatedSelection: string) => void;
  options: Map<string, string>;
};

export const DropDownSelect = ({
  selectedLabel,
  onViewChange,
  options,
}: DropDownSelectProps): React.ReactElement => {
  const dropDownOptions = (): DropdownOption[] => {
    return Array.from(options, ([key, label]) => ({ name: label, value: key }));
  };

  return (
    <Dropdown items={dropDownOptions()} onChange={onViewChange}>
      {selectedLabel}
    </Dropdown>
  );
};
