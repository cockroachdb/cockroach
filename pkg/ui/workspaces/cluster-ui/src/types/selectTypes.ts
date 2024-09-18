// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
