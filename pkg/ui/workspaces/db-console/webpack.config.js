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
    new CopyWebpackPlugin([
      { from: path.resolve(__dirname, "favicon.ico"), to: "favicon.ico" },
    ]),
    // use WebpackBar instead of webpack dashboard to fit multiple webpack dev server outputs (db-console and cluster-ui)
    new WebpackBar({
      name: "db-console",
      color: "orange",
      reporters: [ "basic" ],
      profile: true,
    }),

    // Use MomentTimezoneDataPlugin to remove timezone data that we don't need.
    new MomentTimezoneDataPlugin({
      matchZones: ["Etc/UTC", "America/New_York"],
      startYear: 2021,
      endYear: currentYear + 10,
      // We have to tell the plugin where to store the pruned file
      // otherwise webpack can't find it.
      cacheDir: path.resolve(__dirname, "timezones"),
    }),
  ];

  // First check for local modules, then for third-party modules from
  // node_modules.
  let modules = [
    ...localRoots,
    "node_modules",
  ];

  const config = {
    context: __dirname,
    entry: [ "./src/index.tsx"],
    output: {
      filename: "bundle.js",
      path: path.resolve(env.output || `../../dist${env.dist}`, "assets"),
    },

    mode: argv.mode || "production",

    resolve: {
      // Add resolvable extensions.
      extensions: [".ts", ".tsx", ".js", ".json", ".styl", ".css"],
      modules: modules,
      alias: {
        oss: __dirname,
        ccl: path.resolve(__dirname, "ccl"),
        "src/js/protos": "@cockroachlabs/crdb-protobuf-client",
        "ccl/src/js/protos": "@cockroachlabs/crdb-protobuf-client-ccl",
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
          test: /\.module\.styl$/,
          use: [
            "cache-loader",
            "style-loader",
            {
              loader: "css-loader",
              options: {
                modules: {
                  localIdentName: "[local]--[hash:base64:5]",
                },
              },
            },
            {
              loader: "stylus-loader",
              options: {
                use: [require("nib")()],
              },
            },
          ],
        },
        {
          test: /(?<!\.module)\.styl$/,
          use: [
            "cache-loader",
            "style-loader",
            "css-loader",
            {
              loader: "stylus-loader",
              options: {
                use: [require("nib")()],
              },
            },
          ],
        },
        {
          test: /\.(png|jpg|gif|svg|eot|ttf|woff|woff2)$/,
          loader: "url-loader",
          options: {
            limit: 10000,
            // Preserve the original filename instead of using hash
            // in order to play nice with bazel.
            name: "[name].[ext]",
          },
        },
        { test: /\.html$/, loader: "file-loader" },
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
      contentBase: path.join(__dirname, `dist${env.dist}`),
      index: "",
      proxy: {
        "/": {
          secure: false,
          target: env.target || process.env.TARGET,
          headers: {
            "CRDB-Development": "true",
          },
        },
      },
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
