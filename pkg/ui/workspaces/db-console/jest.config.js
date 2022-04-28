// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
/* global module */
/* eslint-disable @typescript-eslint/no-var-requires */

const { pathsToModuleNameMapper } = require("ts-jest/utils");
const { compilerOptions } = require("./tsconfig.json");

module.exports = {
  moduleFileExtensions: ["ts", "tsx", "js", "jsx", "json", "node"],
  moduleNameMapper: Object.assign(
    {},
    pathsToModuleNameMapper(compilerOptions.paths),
    {
      "\\.(jpg|ico|jpeg|eot|otf|webp|ttf|woff|woff2|mp4|webm|wav|mp3|m4a|aac|oga)$":
        "identity-obj-proxy",
      "\\.(css|scss|less)$": "identity-obj-proxy",
      "\\.(gif|png|svg)$": "<rootDir>/.jest/fileMock.js",
    },
  ),
  moduleDirectories: ["node_modules"],
  modulePaths: ["<rootDir>/"],
  roots: ["<rootDir>/src"],
  testEnvironment: "enzyme",
  setupFilesAfterEnv: ["jest-enzyme", "./enzyme.setup.js"],
  testRegex: "(/__tests__/.*|(\\.|/)(test|spec))\\.tsx?$",
  transformIgnorePatterns: [
    "node_modules/(?!(@cockroachlabs/crdb-protobuf-client)/)",
  ],
  transform: {
    "^.+\\.tsx?$": "ts-jest",
    "^.+\\.jsx?$": "babel-jest",
  },
};
