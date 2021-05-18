// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { Button, Form, Input } from "antd";
import { InputProps } from "antd/lib/input";
import classNames from "classnames/bind";
import {
  Cancel as CancelIcon,
  Search as SearchIcon,
} from "@cockroachlabs/icons";
import styles from "./search.module.scss";

interface ISearchProps {
  onSubmit: (search: string) => void;
  onClear?: () => void;
  defaultValue?: string;
}

interface ISearchState {
  value: string;
  submitted: boolean;
  submit?: boolean;
}

type TSearchProps = ISearchProps & InputProps;

const cx = classNames.bind(styles);

export class Search extends React.Component<TSearchProps, ISearchState> {
  state = {
    value: this.props.defaultValue || "",
    submitted: false,
  };

  onSubmit = (e: React.SyntheticEvent) => {
    e && e.preventDefault();
    const { value } = this.state;
    const { onSubmit } = this.props;
    onSubmit(value);
    if (value.length > 0) {
      this.setState({
        submitted: true,
      });
    }
  };

  onChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const value: string = event.target.value;
    const submitted = value.length === 0;
    this.setState(
      { value, submitted },
      () => submitted && this.onSubmit(event),
    );
  };

  onClear = () => {
    const { onClear } = this.props;
    this.setState({ value: "", submit: false });
    onClear();
  };

  renderSuffix = () => {
    const { value, submitted } = this.state;
    if (value.length > 0) {
      if (submitted) {
        return (
          <Button
            onClick={this.onClear}
            type="default"
            className={cx("clear-search")}
          >
            <CancelIcon className={cx("suffix-icon")} />
          </Button>
        );
      }
      return (
        <Button
          type="default"
          htmlType="submit"
          className={cx("submit-search")}
        >
          Enter
        </Button>
      );
    }
    return null;
  };

  render() {
    const { value, submitted } = this.state;
    const { onClear, ...inputProps } = this.props;
    const className = submitted ? cx("submitted") : "";

    return (
      <Form onSubmit={this.onSubmit} className={cx("search-form")}>
        <Form.Item>
          <Input
            className={className}
            placeholder="Search Statements"
            onChange={this.onChange}
            prefix={<SearchIcon className={cx("prefix-icon")} />}
            suffix={this.renderSuffix()}
            value={value}
            {...inputProps}
          />
        </Form.Item>
      </Form>
    );
  }
}
