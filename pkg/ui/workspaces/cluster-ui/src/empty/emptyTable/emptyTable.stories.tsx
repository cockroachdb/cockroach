// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { storiesOf } from "@storybook/react";
import React from "react";

import { Button } from "src/button";

import emptyListResultsImg from "../../assets/emptyState/empty-list-results.svg";
import notFoundImg from "../../assets/emptyState/not-found-404.svg";
import SpinIcon from "../../icon/spin";

import { EmptyTable } from "./emptyTable";

storiesOf("EmptyTablePlaceholder", module)
  .add("default", () => <EmptyTable />)
  .add("with icon", () => <EmptyTable icon={emptyListResultsImg} />)
  .add("with message", () => (
    <EmptyTable
      icon={emptyListResultsImg}
      message={"Lorem ipsum dolor sit amet, consectetur adipiscing elit."}
    />
  ))
  .add("with button", () => (
    <EmptyTable
      icon={emptyListResultsImg}
      message={"Lorem ipsum dolor sit amet, consectetur adipiscing elit."}
      footer={<Button type="primary">Activate</Button>}
    />
  ))
  .add("no backup found", () => (
    <EmptyTable
      icon={notFoundImg}
      title="No backup found"
      message="The backup you’re looking for does not exist."
    />
  ))
  .add("with SVG element as icon", () => {
    return (
      <EmptyTable
        icon={<SpinIcon width={72} height={72} />}
        title="It's SVG icon"
      />
    );
  });
