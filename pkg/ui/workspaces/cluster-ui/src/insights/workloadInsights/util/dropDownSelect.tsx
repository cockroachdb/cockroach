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
