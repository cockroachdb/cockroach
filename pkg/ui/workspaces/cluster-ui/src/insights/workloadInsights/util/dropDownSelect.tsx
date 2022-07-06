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
import { useHistory, useLocation } from "react-router-dom";
import { viewAttr, tabAttr } from "src/util";
import { queryByName } from "src/util/query";
import { Dropdown, DropdownOption } from "src/dropdown";

type SelectProps = {
  label: string;
  options: { value: string; label: string }[];
};

export const DropDownSelect = ({
  label,
  options,
}: SelectProps): React.ReactElement => {
  const history = useHistory();
  const location = useLocation();
  const tab = queryByName(location, tabAttr);

  const onViewChange = (view: string): void => {
    const searchParams = new URLSearchParams({
      [viewAttr]: view,
    });
    if (tab) {
      searchParams.set(tabAttr, tab);
    }
    history.push({
      search: searchParams.toString(),
    });
  };

  const dropDownOptions = (): DropdownOption[] => {
    return options.map(option => ({
      name: option.label,
      value: option.value,
    }));
  };

  return (
    <Dropdown items={dropDownOptions()} onChange={onViewChange}>
      {label}
    </Dropdown>
  );
};
