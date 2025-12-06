// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

"use strict";

const path = require("path");
const rimraf = require("rimraf");
const webpack = require("webpack");
const CopyWebpackPlugin = require("copy-webpack-plugin");
const StringReplacePlugin = require("string-replace-webpack-plugin");
const MomentTimezoneDataPlugin = require("moment-timezone-data-webpack-plugin");
const WebpackBar = require("webpackbar");
const { ESBuildMinifyPlugin } = require("esbuild-loader");

const currentYear = new Date().getFullYear();

const proxyPrefixes = ["/_admin", "/_status", "/ts", "/login", "/logout"];
function shouldProxy(reqPath) {
  if (reqPath === "/") {
    return true;
  }
  return proxyPrefixes.some(prefix => reqPath.startsWith(prefix));
}

// tslint:disable:object-literal-sort-keys
module.exports = (env, argv) => {
  env = env || {};
  const isBazelBuild = !!process.env.BAZEL_TARGET;

  let localRoots = [path.resolve(__dirname)];
  if (env.dist === "ccl") {
    // CCL modules shadow OSS modules.
    localRoots.unshift(path.resolve(__dirname, "ccl"));
  }

  let plugins = [
    new CopyWebpackPlugin({
      patterns: [
        { from: path.resolve(__dirname, "favicon.ico"), to: "favicon.ico" },
      ],
    }),
    // use WebpackBar instead of webpack dashboard to fit multiple webpack dev server outputs (db-console and cluster-ui)
    new WebpackBar({
      name: "db-console",
      color: "orange",
      reporters: [ "basic" ],
      profile: true,
    }),

    // Use MomentTimezoneDataPlugin to include all timezone data
    new MomentTimezoneDataPlugin({
      startYear: 2021,
      endYear: currentYear + 10,
      // We have to tell the plugin where to store the pruned file
      // otherwise webpack can't find it.
      cacheDir: path.resolve(__dirname, "timezones"),
    }),

    // Webpack 5 no longer provides Node.js polyfills automatically.
    // Provide Buffer and process globals for libraries that expect them.
    new webpack.ProvidePlugin({
      Buffer: ["buffer", "Buffer"],
      process: "process/browser",
    }),
  ];

  // First check for local modules, then for third-party modules from
  // node_modules.
  let modules = [
    ...localRoots,
    "node_modules",
  ];

  const config = {
    devtool: "cheap-source-map",
    context: __dirname,
    entry: [ "./src/index.tsx"],
    output: {
      filename: "bundle.js",
      path: path.resolve(env.output || `../../dist${env.dist}`, "assets"),
    },
    mode: argv.mode || "production",

    resolve: {
      // Add resolvable extensions.
      extensions: [".ts", ".tsx", ".js", ".json", ".scss", ".css"],
      modules: modules,
      alias: {
        oss: __dirname,
        ccl: path.resolve(__dirname, "ccl"),
        "src/js/protos": "@cockroachlabs/crdb-protobuf-client",
        "ccl/src/js/protos": "@cockroachlabs/crdb-protobuf-client-ccl",
        // Webpack 5 doesn't polyfill Node.js built-ins automatically.
        // Some packages import these subpaths explicitly.
        "process/browser": require.resolve("process/browser"),
      },
      // Webpack 5 no longer auto-polyfills Node.js built-ins
      fallback: {
        "path": require.resolve("path-browserify"),
        "fs": false,
        "buffer": require.resolve("buffer/"),
        "stream": require.resolve("stream-browserify"),
        "assert": require.resolve("assert/"),
        "crypto": require.resolve("crypto-browserify"),
        "util": require.resolve("util/"),
        "events": require.resolve("events/"),
        "process": require.resolve("process/browser"),
        "string_decoder": require.resolve("string_decoder/"),
        "http": false,
        "https": false,
        "os": false,
        "url": false,
        "vm": false,
      },
    },

    module: {
      rules: [
        {
          test: /\.css$/,
          use: [
            "style-loader",
            "css-loader",
            {
              loader: "esbuild-loader",
              options: {
                loader: "css",
                minify: true,
              },
            }
          ]
        },
        // Preprocess LESS styles required by external components
        // (react-select)
        {
          use: [
            "style-loader",
            "css-loader",
            {
              loader: "less-loader",
              options: {
                lessOptions: {
                  javascriptEnabled: true
                }
              }
            },
          ],
          test: /\.less$/,
        },
        {
          test: /\.module\.scss$/,
          use: [
            "style-loader",
            {
              loader: "css-loader",
              options: {
                modules: {
                  localIdentName: "[local]--[hash:base64:5]",
                },
              },
            },
            "sass-loader",
          ],
        },
        {
          test: /(?<!\.module)\.scss$/,
          use: [
            "style-loader",
            "css-loader",
            "sass-loader",
          ],
        },
        {
          test: /\.(png|jpg|gif|svg|eot|ttf|woff|woff2)$/,
          type: "asset",
          parser: {
            dataUrlCondition: {
              maxSize: 10 * 1024, // 10kb
            },
          },
          generator: {
            // Preserve the original filename instead of using hash
            // in order to play nice with bazel.
            filename: "[name][ext]",
          },
        },
        {
          test: /\.html$/,
          type: "asset/resource",
        },
        {
          test: /\.js$/,
          include: localRoots,
          exclude: [
            /node_modules/,
            /src\/js/,
            /ccl\/src\/js/,
            /cluster-ui\/dist/,
          ],
          use: [
            {
              loader: "esbuild-loader",
              options: {
                loader: "js",
                target: "es6",
              },
            }
          ],
        },
        {
          test: /\.(ts|tsx)?$/,
          include: localRoots,
          exclude: [
            /node_modules/,
            /src\/js/,
            /ccl\/src\/js/,
            /cluster-ui\/dist/,
          ],
          use: [
            {
              loader: "esbuild-loader",
              options: {
                loader: "tsx",
                target: "es6",
              },
            },
          ],
        },

        // All output ".js" files will have any sourcemaps re-processed by "source-map-loader".
        {
          enforce: "pre",
          test: /\.js$/,
          loader: "source-map-loader",
          include: localRoots,
          exclude: [
            /node_modules/,
            /src\/js/,
            /ccl\/src\/js/,
            /cluster-ui\/dist/,
          ],
        },
      ],
    },

    optimization: {
      minimizer: [
        new ESBuildMinifyPlugin({
          target: "es6",
        }),
      ],
    },

    plugins: plugins,

    stats: "errors-only",

    devServer: {
      static: {
        directory: path.join(__dirname, `dist${env.dist}`),
      },
      devMiddleware: {
        index: false,
      },
      proxy: [
        {
          context: ["/"],
          secure: false,
          target: env.target || process.env.TARGET,
          headers: {
            "CRDB-Development": "true",
          },
        },
      ],
    },

    watchOptions: {
      poll: 1000,
      ignored: /node_modules/,
    },

    // Max size of is set to 4Mb to disable warning message and control
    // the growing size of bundle over time.
    performance: {
      maxEntrypointSize: 4000000,
      maxAssetSize: 4000000,
    },
  };

  // The purpose of this is to have zero impact on production code and at the
  // same time to be able inject testing code with SIMULATE_METRICS_LOAD env variable.
  // It injects Redux middleware which generates metrics datapoints for performance
  // testing.
  if (
    process.env.SIMULATE_METRICS_LOAD === "true" &&
    argv.mode !== "production"
  ) {
    config.module.rules.push({
      test: /src\/redux\/state.ts$/,
      loader: StringReplacePlugin.replace({
        replacements: [
          {
            pattern: /import rootSaga from ".\/sagas";/gi, // match last 'import' expression in module.
            replacement: function(match, p) {
              return `${match}\nimport { fakeMetricsDataGenerationMiddleware } from "src/test-utils/fakeMetricsDataGenerationMiddleware";`;
            },
          },
          {
            pattern: /(?<=applyMiddleware\((.*))\),$/gm,
            replacement: function(match) {
              return `, fakeMetricsDataGenerationMiddleware${match}`;
            },
          },
        ],
      }),
    });
    config.plugins.push(new StringReplacePlugin());
  }
  return config;
};
