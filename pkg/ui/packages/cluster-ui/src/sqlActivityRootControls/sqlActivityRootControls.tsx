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
import { viewAttr, tabAttr } from "src/util";
import { useHistory, useLocation } from "react-router-dom";
import { queryByName } from "src/util/query";
import {
  SelectWithDescription,
  Option,
} from "src/selectWithDescription/selectWithDescription";

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

  const content = options.find((option: Option) => option.value === viewValue)
    .component;

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
