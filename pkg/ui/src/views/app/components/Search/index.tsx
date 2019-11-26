// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Button, Form, Icon, Input } from "antd";
import { InputProps } from "antd/lib/input";
import React from "react";
import "./search.styl";

interface ISearchProps {
  onSubmit: (value: string) => void;
  onClear?: () => void;
}

interface ISearchState {
  value: string;
  submitted: boolean;
  submit?: boolean;
}

type TSearchProps = ISearchProps & InputProps;

export class Search extends React.Component<TSearchProps, ISearchState> {
  state = {
    value: "",
    submitted: false,
  };

  onSubmit = () => {
    const { value } = this.state;
    const { onSubmit } = this.props;
    onSubmit(value);
    if (value.length > 0) {
      this.setState({
        submitted: true,
      });
    }
  }

  onChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const value: string = event.target.value;
    const submitted = value.length === 0;
    this.setState({ value, submitted }, () => submitted && this.onSubmit());
  }

  onClear = () => {
    const { onClear } = this.props;
    this.setState({ value: "", submit: false });
    onClear();
  }

  renderSuffix = () => {
    const { value, submitted } = this.state;
    if (value.length > 0) {
      if (submitted) {
        return <Button onClick={this.onClear} type="default" className="_clear-search" icon="close" />;
      }
      return <Button type="default" htmlType="submit" className="_submit-search">Enter</Button>;
    }
    return null;
  }

  render() {
    const { value, submitted } = this.state;
    const className = submitted ? "_submitted" : "";

    /*
      current antdesign (3.25.3) has Input.d.ts incompatible with latest @types/react
      temporary fix, remove as soon antd can be updated
    */

    // tslint:disable-next-line: variable-name
    const MyInput = Input as any;

    return (
      <Form onSubmit={this.onSubmit} className="_search-form">
        <Form.Item>
          <MyInput
            className={className}
            placeholder="Search Statement"
            onChange={this.onChange}
            prefix={<Icon className="_prefix-icon" type="search" />}
            suffix={this.renderSuffix()}
            value={value}
            {...this.props}
         />
        </Form.Item>
      </Form>
    );
  }
}
