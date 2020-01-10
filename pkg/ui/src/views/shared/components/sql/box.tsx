// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";
import React from "react";
import { Highlight } from "./highlight";
import "./sqlhighlight.styl";
import TableInfo from "./tableInfo";

interface SqlBoxProps {
  value: string;
}
interface SqlBoxState {
  code: Array<string>;
}

const regExpTables = /(?<=(FROM|INTO|TABLE|UPDATE|CREATE)\s)([^\s([]+)/g;
const regExpList = /FROM|INTO|TABLE|UPDATE|CREATE/g;

class SqlBox extends React.Component<SqlBoxProps, SqlBoxState> {
  state = {
    code: [""],
  };

  preNode: React.RefObject<HTMLPreElement> = React.createRef();

  componentDidMount() {
    this.replaceTableData();
  }

  getTableData = (value: string) => {
    if (!regExpTables.test(value)) {
      return value;
    }
    return value.match(regExpTables)[0];
  }

  replaceSystemData = (value: string) => {
    if (!regExpList.test(value)) {
    return <Highlight value={value} />;
    } else {
      const data = _.replace(value, this.getTableData(value), "");
      // tslint:disable-next-line: no-shadowed-variable
      const dataValues = _.replace(data, regExpList, (value: string) => `${value}splitIt`);
      const code = dataValues.split(/splitIt /g);
      return code.map((stringElement) => (
        <React.Fragment>
          <Highlight value={` ${stringElement} `} />
          <span>
            {regExpList.test(stringElement) && (
              <TableInfo
                title={this.getTableData(value)}
                params={{
                  database_name: this.getTableData(value).split(".")[0],
                  table_name: this.getTableData(value).split(".")[this.getTableData(value).split(".").length - 1],
                }}
              />
            )}
          </span>
        </React.Fragment>
      ));
    }
  }

  replaceTableData = () => {
    let val = this.props.value;
    val = _.replace(val, regExpList, (value: string) => `splitIt${value}`);
    const code = val.split(/ splitIt/g);
    this.setState({ code });
  }

  render() {
    const { code } = this.state;
    return (
      <div className="box-highlight">
        {code && code.map((value: string) => this.replaceSystemData(value))}
      </div>
    );
  }
}
export default SqlBox;
