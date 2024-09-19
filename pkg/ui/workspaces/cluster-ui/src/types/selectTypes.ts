// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This is temporary. We'll remove this when we can access the shared
// component library from the console. In the meantime this just removes
// some type pollution from new components using react-select.
export type ReactSelectOption<T> = {
  label: string;
  value: T;
};

export type GroupedReactSelectOption<T> = {
  label: string;
  options: ReactSelectOption<T>[];
};
