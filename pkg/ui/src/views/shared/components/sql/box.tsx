// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { Highlight } from "./highlight";
import "./sqlhighlight.styl";

interface SqlBoxProps {
  value: string;
}
class SqlBox extends React.Component<SqlBoxProps> {
  preNode: React.RefObject<HTMLPreElement> = React.createRef();
  render() {
    const { value } = this.props;
    return (
      <div className="box-highlight">
        <Highlight value={value} />
      </div>
    );
  }
}
export default SqlBox;
