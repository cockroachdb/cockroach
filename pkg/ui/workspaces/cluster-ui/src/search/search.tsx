// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  Cancel as CancelIcon,
  Search as SearchIcon,
} from "@cockroachlabs/icons";
import { Button, Input, ConfigProvider } from "antd";
import classNames from "classnames/bind";
import noop from "lodash/noop";
import React from "react";

import { crlTheme } from "../antdTheme";

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
    e?.preventDefault && e.preventDefault();
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
    this.setState({ value, submitted });
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
            type="text"
            className={cx("clear-search")}
            size="small"
          >
            <CancelIcon className={cx("suffix-icon")} />
          </Button>
        );
      }
      return (
        <Button
          type="text"
          onClick={this.onSubmit}
          className={cx("submit-search")}
          size="small"
        >
          Enter
        </Button>
      );
    }
    return null;
  };

  render(): React.ReactElement {
    const { value } = this.state;
    // We pull out onSubmit and onClear so that they will not be passed
    // to the Input component as part of inputProps.
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const { onSubmit, onClear, onChange, ...inputProps } = this.props;

    return (
      <ConfigProvider theme={crlTheme}>
        <Input
          className={cx("root")}
          onChange={this.onChange}
          onPressEnter={this.onSubmit}
          prefix={<SearchIcon className={cx("prefix-icon")} />}
          suffix={this.renderSuffix()}
          value={value}
          size={"small"}
          {...inputProps}
        />
      </ConfigProvider>
    );
  }
}
