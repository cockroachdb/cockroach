// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useMemo } from "react";
import { viewAttr, tabAttr } from "src/util";
import { useHistory } from "react-router-dom";
import { useQueryParmeters } from "../hooks";
import {
  SelectWithDescription,
  Option,
} from "src/selectWithDescription/selectWithDescription";

export type SQLActivityContentRootProps = {
  options: Option[];
};

export const SQLActivityContentRoot = ({
  options,
}: SQLActivityContentRootProps): React.ReactElement => {
  const history = useHistory();
  const searchParams = useQueryParmeters();
  const viewValue = searchParams.get(viewAttr) || options[0].value;

  const onViewChange = (view: string): void => {
    const newParams = new URLSearchParams({
      [viewAttr]: view,
      [tabAttr]: searchParams.get(tabAttr),
    });

    history.push({
      ...history.location,
      search: newParams.toString(),
    });
  };

  const Content = useMemo(() => {
    return options.find((option: Option) => option.value === viewValue)
      .component;
  }, [options, viewValue]);

  return (
    <div>
      <SelectWithDescription
        options={options}
        value={viewValue}
        onChange={onViewChange}
      />
      <Content />
    </div>
  );
};
