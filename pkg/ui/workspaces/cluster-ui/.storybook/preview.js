// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import * as protobuf from "protobufjs/minimal";
import Long from "long";

protobuf.util.Long = Long;
protobuf.configure();

export const parameters = {
  backgrounds: {
    default: 'cockroachdb',
    values: [
      {
        name: 'cockroachdb',
        value: '#F5F7FA',
      },
    ],
  },
};
