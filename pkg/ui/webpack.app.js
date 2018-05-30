"use strict";

const path = require("path");
const rimraf = require("rimraf");
const webpack = require("webpack");
const CopyWebpackPlugin = require("copy-webpack-plugin");

// Remove a broken dependency that Yarn insists upon installing before every
// Webpack compile. We also do this when installing dependencies via Make, but
// it"s common to run e.g. `yarn add` manually without re-running Make, which
// will reinstall the broken dependency. The error this dependency causes is
// horribly cryptic, so it"s important to remove it aggressively.
//
// See: https://github.com/yarnpkg/yarn/issues/2987
class RemoveBrokenDependenciesPlugin {
  apply(compiler) {
    compiler.plugin("compile", () => rimraf.sync("./node_modules/@types/node"));
  }
}

let DashboardPlugin;
try {
  DashboardPlugin = require("./opt/node_modules/webpack-dashboard/plugin");
} catch (e) {
  DashboardPlugin = class { apply() { /* no-op */ } };
}

const proxyPrefixes = ["/_admin", "/_status", "/ts", "/login", "/logout"];
function shouldProxy(reqPath) {
  if (reqPath === "/") {
    return true;
  }
  return proxyPrefixes.some((prefix) => (
    reqPath.startsWith(prefix)
  ));
}

// tslint:disable:object-literal-sort-keys
module.exports = (distDir, ...additionalRoots) => ({
  entry: ["./src/index.tsx"],
  output: {
    filename: "bundle.js",
    path: path.resolve(__dirname, distDir),
  },

  resolve: {
    // Add resolvable extensions.
    extensions: [".ts", ".tsx", ".js", ".json", ".styl", ".css"],
    // Resolve modules from src directory or node_modules.
    // The "path.resolve" is used to make these into absolute paths, meaning
    // that only the exact directory is checked. A relative path would follow
    // the resolution behavior used by node.js for "node_modules", which checks
    // for a "node_modules" child directory located in either the current
    // directory *or in any parent directory*.
    modules: [
      ...additionalRoots,
      path.resolve(__dirname),
      path.resolve(__dirname, "node_modules"),
    ],
    alias: {oss: path.resolve(__dirname)},
  },

  module: {
    rules: [
      { test: /\.css$/, use: [ "style-loader", "css-loader" ] },
      {
        test: /\.styl$/,
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
        },
      },
      { test: /\.html$/, loader: "file-loader" },
      {
        test: /\.js$/,
        include: [...additionalRoots, path.resolve(__dirname, "src")],
        use: ["cache-loader", "babel-loader"],
      },
      {
        test: /\.tsx?$/,
        include: [...additionalRoots, path.resolve(__dirname, "src")],
        use: [
          "cache-loader",
          "babel-loader",
          { loader: "ts-loader", options: { happyPackMode: true } },
        ],
      },

      // All output ".js" files will have any sourcemaps re-processed by "source-map-loader".
      { enforce: "pre", test: /\.js$/, loader: "source-map-loader" },
    ],
  },

  plugins: [
    new RemoveBrokenDependenciesPlugin(),
    // See "DLLs for speedy builds" in the README for details.
    new webpack.DllReferencePlugin({
      manifest: require("./protos-manifest.json"),
    }),
    new webpack.DllReferencePlugin({
      manifest: require("./vendor-manifest.json"),
    }),
    new CopyWebpackPlugin([{ from: "favicon.ico", to: "favicon.ico" }]),
    new DashboardPlugin(),
  ],

  // https://webpack.js.org/configuration/stats/
  stats: {
    colors: true,
    chunks: false,
  },

  devServer: {
    contentBase: path.join(__dirname, distDir),
    index: "",
    proxy: {
      // Note: this shouldn't require a custom bypass function to work;
      // docs say that setting `index: ''` is sufficient to proxy `/`.
      // However, that did not work, and may require upgrading to webpack 4.x.
      "/": {
        secure: false,
        target: process.env.TARGET,
        bypass: (req) => {
          if (shouldProxy(req.path)) {
            return false;
          }
          return req.path;
        },
      },
    },
  },
});
