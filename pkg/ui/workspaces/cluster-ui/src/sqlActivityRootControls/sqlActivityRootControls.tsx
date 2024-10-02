// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { useHistory, useLocation } from "react-router-dom";

import {
  SelectWithDescription,
  Option,
} from "src/selectWithDescription/selectWithDescription";
import { viewAttr, tabAttr } from "src/util";
import { queryByName } from "src/util/query";

export type SQLActivityRootControlsProps = {
  options: Option[];
};

// SQLActivityRootControls is used by the Statement and Transaction tabs
// to render content based on the options provided via the props and
// the user's selection.
export const SQLActivityRootControls = ({
  options,
}: SQLActivityRootControlsProps): React.ReactElement => {
  const history = useHistory();
  const location = useLocation();
  const viewValue = queryByName(location, viewAttr) || options[0].value;
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

  const content = options.find(
    (option: Option) => option.value === viewValue,
  ).component;

  return (
    <div>
      <SelectWithDescription
        options={options}
        value={viewValue}
        onChange={onViewChange}
      />
      {content}
    </div>
  );
};
