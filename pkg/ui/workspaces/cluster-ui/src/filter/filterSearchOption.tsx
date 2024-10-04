// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { Search } from "../search";
import { filterLabel } from "../queryFilter/filterClasses";

export type FilterSearchOptionProps = {
  label: string;
  onChanged?: (value: string) => void;
  value?: string;
};

export const FilterSearchOption = (
  props: FilterSearchOptionProps,
): React.ReactElement => {
  const { label, onChanged, value } = props;
  return (
    <div>
      <div className={filterLabel.margin}>{label}</div>
      <Search
        onChange={onChanged}
        suffix={false}
        placeholder="Search"
        value={value}
      />
    </div>
  );
};
