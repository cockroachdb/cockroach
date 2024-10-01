// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

const path = require("path");
const isBazel = !!process.env.BAZEL_TARGET;

const bazelOnlySettings = {
  haste: {
    // Platforms that include a POSIX-compatible `find` binary default to using it for test file
    // discovery, but jest-haste-map's invocation of `find` doesn't include `-L` when node was
    // started with `--preserve-symlinks`. This causes Jest to be unable to find test files when run
    // via Bazel, which uses readonly symlinks for its build sandbox and launches node with
    // `--presrve-symlinks`. Use jest's pure-node implementation instead, which respects
    // `--preserve-symlinks`.
    forceNodeFilesystemAPI: true,
    enableSymlinks: true,
  },
  watchman: false,
};

module.exports = {
  moduleFileExtensions: ["ts", "tsx", "js", "jsx", "json", "node"],
  moduleNameMapper: {
    "\\.(jpg|ico|jpeg|eot|otf|webp|ttf|woff|woff2|mp4|webm|wav|mp3|m4a|aac|oga)$": "identity-obj-proxy",
    "\\.(css|scss|less)$": "identity-obj-proxy",
    "\\.(gif|png|svg)$": "<rootDir>/.jest/fileMock.js",
  },
  moduleDirectories: [
    "node_modules"
  ],
  modulePaths: [
    "<rootDir>/"
  ],
  roots: ["<rootDir>/src"],
  testEnvironment: "enzyme",
  setupFilesAfterEnv: ["./enzyme.setup.js", "./src/test-utils/matchMedia.mock.js", "jest-enzyme", "jest-canvas-mock"],
  transform: {
    "^.+\\.tsx?$": "ts-jest",
    "^.+\\.jsx?$": ['babel-jest', { configFile: path.resolve(__dirname, 'babel.config.js') }],
  },
  transformIgnorePatterns: [
  ],
  ...( isBazel ? bazelOnlySettings : {} ),
};
