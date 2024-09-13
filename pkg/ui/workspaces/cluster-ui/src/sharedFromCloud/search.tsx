// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Icon } from "@cockroachlabs/ui-components";
import React from "react";

import Button from "./button";
import Input from "./input";

import "./search.scss";

interface SearchProps {
  onSubmit: (search: string) => void;
  defaultValue?: string;
  placeholder?: string;
}

export function Search(props: SearchProps) {
  const { onSubmit, defaultValue, placeholder } = props;
  const [value, setValue] = React.useState(defaultValue || "");
  const [submittedValue, setSubmittedValue] = React.useState(value);

  const doSubmit = (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setSubmittedValue(value);
    onSubmit(value);
  };
  const doClear = () => {
    setValue("");
    setSubmittedValue("");
    onSubmit("");
  };
  const hasChanges = value !== submittedValue;

  let suffix;
  if (hasChanges) {
    suffix = (
      <Button
        category="flat"
        actionType="submit"
        text="Enter"
        className="search__button"
      />
    );
  } else if (value !== "") {
    suffix = (
      <Button
        category="flat"
        actionType="button"
        icon={<Icon iconName="Cancel" size="tiny" />}
        onClick={doClear}
        className="search__button"
      />
    );
  }

  return (
    <form onSubmit={doSubmit} className="search">
      <Input
        type="text"
        placeholder={placeholder || "Search"}
        prefix={<Icon iconName="Search" size="tiny" />}
        suffix={suffix}
        value={value}
        onChange={e => setValue(e.target.value)}
      />
    </form>
  );
}
