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
