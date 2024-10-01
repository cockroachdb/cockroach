// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Button, commonStyles } from "@cockroachlabs/cluster-ui";
import { ArrowLeft } from "@cockroachlabs/icons";
import { History } from "history";
import React from "react";

interface backProps {
  history: History;
}
export function BackToAdvanceDebug(props: backProps): React.ReactElement {
  function onBackClick(history: History): void {
    history.push("/debug");
  }

  return (
    <Button
      onClick={() => onBackClick(props.history)}
      type="unstyled-link"
      size="small"
      icon={<ArrowLeft fontSize={"10px"} />}
      iconPosition="left"
      className={commonStyles("small-margin")}
    >
      Advanced Debug
    </Button>
  );
}
