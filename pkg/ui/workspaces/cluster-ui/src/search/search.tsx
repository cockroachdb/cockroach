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
import React, { useState, useCallback } from "react";

import { crlTheme } from "../antdTheme";

import styles from "./search.module.scss";

import type { InputProps } from "antd/lib/input";

interface ISearchProps {
  onSubmit?: (search: string) => void;
  onChange?: (value: string) => void;
  onClear?: () => void;
  defaultValue?: string;
  placeholder?: string;
  suffix?: boolean;
}

type TSearchProps = ISearchProps &
  Omit<InputProps, "onSubmit" | "defaultValue" | "placeholder" | "onChange">; // Omit shadowed props by ISearchProps type.

const cx = classNames.bind(styles);

export function Search({
  onSubmit = noop,
  onChange = noop,
  onClear = noop,
  defaultValue = "",
  placeholder = "Search Statements",
  suffix,
  ...inputProps
}: TSearchProps): React.ReactElement {
  const [value, setValue] = useState(defaultValue || "");
  const [submitted, setSubmitted] = useState(false);

  const handleSubmit = useCallback(
    (e: React.SyntheticEvent): void => {
      e?.preventDefault && e.preventDefault();
      onSubmit(value);
      if (value.length > 0) {
        setSubmitted(true);
      }
    },
    [value, onSubmit],
  );

  const handleChange = useCallback(
    (event: React.ChangeEvent<HTMLInputElement>): void => {
      const newValue: string = event.target.value;
      const isSubmitted = newValue.length === 0;
      onChange(newValue);
      setValue(newValue);
      setSubmitted(isSubmitted);
    },
    [onChange],
  );

  const handleClear = useCallback((): void => {
    setValue("");
    setSubmitted(false);
    onClear();
  }, [onClear]);

  const renderSuffix = (): React.ReactElement => {
    if (suffix === false) {
      return null;
    }
    if (value.length > 0) {
      if (submitted) {
        return (
          <Button
            onClick={handleClear}
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
          onClick={handleSubmit}
          className={cx("submit-search")}
          size="small"
        >
          Enter
        </Button>
      );
    }
    return null;
  };

  return (
    <ConfigProvider theme={crlTheme}>
      <Input
        className={cx("root")}
        onChange={handleChange}
        onPressEnter={handleSubmit}
        prefix={<SearchIcon className={cx("prefix-icon")} />}
        suffix={renderSuffix()}
        value={value}
        placeholder={placeholder}
        size={"small"}
        {...inputProps}
      />
    </ConfigProvider>
  );
}
