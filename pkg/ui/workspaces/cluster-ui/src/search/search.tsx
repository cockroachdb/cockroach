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
import "antd/lib/button/style";
import "antd/lib/form/style";
import "antd/lib/input/style";
import classNames from "classnames/bind";
import noop from "lodash/noop";
import {
  Cancel as CancelIcon,
  Search as SearchIcon,
} from "@cockroachlabs/icons";

import styles from "./search.module.scss";

import type { InputProps } from "antd/lib/input";

interface ISearchProps {
  onSubmit: (search: string) => void;
  onChange?: (value: string) => void;
  onClear?: () => void;
  defaultValue?: string;
  placeholder?: string;
  suffix?: boolean;
}

interface ISearchState {
  value: string;
  submitted: boolean;
  submit?: boolean;
}

type TSearchProps = ISearchProps &
  Omit<InputProps, "onSubmit" | "defaultValue" | "placeholder" | "onChange">; // Omit shadowed props by ISearchProps type.

const cx = classNames.bind(styles);

export class Search extends React.Component<TSearchProps, ISearchState> {
  static defaultProps: Partial<ISearchProps> = {
    placeholder: "Search Statements",
    onSubmit: noop,
    onChange: noop,
    onClear: noop,
  };

  state: ISearchState = {
    value: this.props.defaultValue || "",
    submitted: false,
  };

  onSubmit = (e: React.SyntheticEvent): void => {
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

  onChange = (event: React.ChangeEvent<HTMLInputElement>): void => {
    event.persist();
    const value: string = event.target.value;
    const submitted = value.length === 0;
    this.props.onChange(value);
    this.setState(
      { value, submitted },
      () => submitted && this.onSubmit(event),
    );
  };

  onClear = (): void => {
    const { onClear } = this.props;
    this.setState({ value: "", submit: false });
    onClear();
  };

  renderSuffix = (): React.ReactElement => {
    if (this.props.suffix === false) {
      return null;
    }
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

  render(): React.ReactElement {
    const { value, submitted } = this.state;
    // We pull out onSubmit and onClear so that they will not be passed
    // to the Input component as part of inputProps.
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const { onSubmit, onClear, onChange, ...inputProps } = this.props;
    const className = submitted ? cx("submitted") : "";

    return (
      <Form onSubmit={this.onSubmit} className={cx("search-form")}>
        <Form.Item>
          <Input
            className={className}
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
