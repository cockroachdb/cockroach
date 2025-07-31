// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
/* global module, __dirname, process */
/* eslint-disable @typescript-eslint/no-var-requires */

const path = require("path");
const { pathsToModuleNameMapper } = require("ts-jest");
const isBazel = !!process.env.BAZEL_TARGET;
const srcDir = process.cwd();
const { compilerOptions } = require(path.resolve(srcDir, "./tsconfig.json"));

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

/*
 * For a detailed explanation regarding each configuration property, visit:
 * https://jestjs.io/docs/configuration
 */
module.exports = {
  // All imported modules in your tests should be mocked automatically
  // automock: false,

  // Stop running tests after `n` failures
  // bail: 0,

  // The directory where Jest should store its cached dependency information
  // cacheDirectory: "/private/var/folders/hs/c4079dx544nfw1z_951l0nc00000gq/T/jest_dz",

  // Automatically clear mock calls, instances, contexts and results before every test
  clearMocks: true,

  // Indicates whether the coverage information should be collected while executing the test
  // collectCoverage: false,

  // An array of glob patterns indicating a set of files for which coverage information should be collected
  // collectCoverageFrom: undefined,

  // The directory where Jest should output its coverage files
  // coverageDirectory: undefined,

  // An array of regexp pattern strings used to skip coverage collection
  // coveragePathIgnorePatterns: [
  //   "/node_modules/"
  // ],

  // Indicates which provider should be used to instrument code for coverage
  coverageProvider: "v8",

  // A list of reporter names that Jest uses when writing coverage reports
  // coverageReporters: [
  //   "json",
  //   "text",
  //   "lcov",
  //   "clover"
  // ],

  // An object that configures minimum threshold enforcement for coverage results
  // coverageThreshold: undefined,

  // A path to a custom dependency extractor
  // dependencyExtractor: undefined,

  // Make calling deprecated APIs throw helpful error messages
  // errorOnDeprecated: false,

  // The default configuration for fake timers
  // fakeTimers: {
  //   "enableGlobally": false
  // },

  // Force coverage collection from ignored files using an array of glob patterns
  // forceCoverageMatch: [],

  // A path to a module which exports an async function that is triggered once before all test suites
  // globalSetup: undefined,

  // A path to a module which exports an async function that is triggered once after all test suites
  // globalTeardown: undefined,

  // A set of global variables that need to be available in all test environments
  globals: {
    "ts-jest": {
      tsconfig: path.resolve(srcDir, "./tsconfig.linting.json"),
    },
  },

  // The maximum amount of workers used to run your tests. Can be specified as % or a number. E.g. maxWorkers: 10% will use 10% of your CPU amount + 1 as the maximum worker number. maxWorkers: 2 will use a maximum of 2 workers.
  // maxWorkers: "50%",

  // An array of directory names to be searched recursively up from the requiring module's location
  moduleDirectories: ["node_modules"],

  // An array of file extensions your modules use
  moduleFileExtensions: ["ts", "tsx", "js", "jsx", "json", "node"],

  // A map from regular expressions to module names or to arrays of module names that allow to stub out resources with a single module
  moduleNameMapper: Object.assign(
    {},
    pathsToModuleNameMapper(
      // compilerOptions.paths
      // The TypeScript compiler needs to know how to find Bazel-produced .d.ts
      // files, but those overrides break Jest's module loader. Remove the
      // @cockroachlabs entries from tsconfig.json's 'paths' object.
      Object.fromEntries(
        Object.entries(compilerOptions.paths).filter(([name, _paths]) => !name.includes("@cockroachlabs"))
      ), { prefix: "<rootDir>/" }
    ),
    {
      "\\.(jpg|ico|jpeg|eot|otf|webp|ttf|woff|woff2|mp4|webm|wav|mp3|m4a|aac|oga|gif|png|svg)$":
        "<rootDir>/src/test-utils/file.mock.js",
      "\\.(css|scss|less|styl)$": "identity-obj-proxy",
      "^react($|/.+)": "<rootDir>/node_modules/react$1",
    },
  ),

  // An alternative API to setting the NODE_PATH env variable, modulePaths is an array of absolute paths to additional locations to search when resolving modules.
  modulePaths: ["<rootDir>/"],

  // An array of regexp pattern strings, matched against all module paths before considered 'visible' to the module loader
  // modulePathIgnorePatterns: [],

  // Activates notifications for test results
  // notify: false,

  // An enum that specifies notification mode. Requires { notify: true }
  // notifyMode: "failure-change",

  // A preset that is used as a base for Jest's configuration
  preset: "ts-jest",

  // Run tests from one or more projects
  // projects: undefined,

  // Use this configuration option to add custom reporters to Jest
  // reporters: [],

  // Automatically reset mock state before every test
  // resetMocks: false,

  // Reset the module registry before running each individual test
  // resetModules: false,

  // A path to a custom resolver
  // resolver: undefined,

  // Automatically restore mock state and implementation before every test
  // restoreMocks: false,

  // The root directory that Jest should scan for tests and modules within
  // rootDir: undefined,

  // A list of paths to directories that Jest should use to search for files in
  roots: ["<rootDir>", "<rootDir>/src", "<rootDir>/ccl"],

  // Allows you to use a custom runner instead of Jest's default test runner
  // runner: "jest-runner",

  // The paths to modules that run some code to configure or set up the testing environment
  // before each test.
  // setupFiles: [],

  // A list of paths to modules that run some code to configure or set up the testing framework
  // before each test. These run after the test environment is setup for each test. This
  setupFilesAfterEnv: [
    "jest-canvas-mock",
    "jest-enzyme",
    "<rootDir>/src/setupTests.js",
  ],

  // The number of seconds after which a test is considered as slow and reported as such in the results.
  // slowTestThreshold: 5,

  // A list of paths to snapshot serializer modules Jest should use for snapshot testing
  // snapshotSerializers: [],

  // The test environment that will be used for testing
  testEnvironment: "jsdom",

  // Options that will be passed to the testEnvironment
  // testEnvironmentOptions: {},

  // Adds a location field to test results
  // testLocationInResults: false,

  // The glob patterns Jest uses to detect test files
  // testMatch: [
  //   "**/__tests__/**/*.[jt]s?(x)",
  //   "**/?(*.)+(spec|test).[tj]s?(x)"
  // ],

  // An array of regexp pattern strings that are matched against all test paths, matched tests are skipped
  testPathIgnorePatterns: ["/node_modules/"],

  // The regexp pattern or array of patterns that Jest uses to detect test files
  // testRegex: [],

  // This option allows the use of a custom results processor
  // testResultsProcessor: undefined,

  // This option allows use of a custom test runner
  // testRunner: "jest-circus/runner",

  // A map from regular expressions to paths to transformers
  transform: {
    "^.+\\.tsx?$": "ts-jest",
    "^.+\\.js?$": [
      "babel-jest",
	{ configFile: path.resolve(srcDir, "babel.config.js") },
    ],
  },

  // An array of regexp pattern strings that are matched against all source file paths, matched files will skip transformation
  transformIgnorePatterns: [
    "/node_module\\/@cockroachlabs\\/crdb-protobuf-client/",
    "/node_module\\/@cockroachlabs\\/cluster-ui/",
    "/cluster-ui\\/dist\\/js\\/main.js$/",
  ],

  // An array of regexp pattern strings that are matched against all modules before the module loader will automatically return a mock for them
  // unmockedModulePathPatterns: undefined,

  // Indicates whether each individual test should be reported during the run
  // verbose: undefined,

  // An array of regexp patterns that are matched against all source file paths before re-running tests in watch mode
  // watchPathIgnorePatterns: [],

  // Whether to use watchman for file crawling
  // watchman: true,
  ...(isBazel ? bazelOnlySettings : {}),
};
