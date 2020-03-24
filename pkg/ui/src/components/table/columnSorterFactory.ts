// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { isFunction, isUndefined } from "lodash";

export function columnSorterFactory<T>(field: keyof T | ((column: T) => string|number)) {
  return (a: T, b: T) => {
    let fieldA, fieldB;
    if (isFunction(field)) {
      fieldA = field(a);
      fieldB = field(b);
    } else {
      fieldA = a[field];
      fieldB = b[field];
    }

    if (isUndefined(fieldA) && !isUndefined(fieldB)) { return -1; }
    if (!isUndefined(fieldA) && isUndefined(fieldB)) { return 1; }
    if (isUndefined(fieldA) || isUndefined(fieldB)) { return 0; }

    if (fieldA < fieldB) { return -1; }
    if (fieldA > fieldB) { return 1; }
    return 0;
  };
}
