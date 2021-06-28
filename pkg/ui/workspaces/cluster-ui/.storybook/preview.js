// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
