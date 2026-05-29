// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

declare module "fetch-mock";
declare module "react-paginate";

declare module "react-select" {
  import * as React from "react";

  export interface Option<T = string> {
    value?: T;
    label?: string;
    disabled?: boolean;
    clearableValue?: boolean;
    [key: string]: any;
  }

  export interface OptionComponentProps<T = string> {
    option: Option<T>;
    className?: string;
    onSelect: (option: Option<T>, event: any) => void;
    onFocus: (option: Option<T>, event: any) => void;
    isFocused?: boolean;
    isSelected?: boolean;
    [key: string]: any;
  }

  interface SelectProps {
    className?: string;
    clearable?: boolean;
    searchable?: boolean;
    options?: any[];
    value?: any;
    onChange?: (value: any) => void;
    onFocus?: () => void;
    onClose?: () => void;
    arrowRenderer?: (props: { isOpen: boolean }) => React.ReactElement;
    ref?: React.Ref<any>;
    [key: string]: any;
  }

  const Select: React.ComponentType<SelectProps>;
  export default Select;
}
