// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

const fs = require("fs");
const fsp = require("fs/promises");
const os = require("os");
const path = require("path");
const semver = require("semver");
const { validate } = require("schema-utils");

const PLUGIN_NAME = `CopyEmittedFilesPlugin`;

const SCHEMA = {
  type: "object",
  additionalProperties: false,
  properties: {
    destinations: {
      description: "Destination directories to copy emitted files to.",
      type: "array",
      items: {
        type: "string",
      },
      // Disallow duplicates.
      uniqueItems: true,
    },
  },
};

/**
 * A webpack plugin that copies emitted files to additional directories.
 * Its main purpose is to allow a watch-mode compilation to continuously "push"
 * output files into external directories. This avoids the "multiple/mismatched
 * copies of dependency $foo" behavior that often comes with symlink-based
 * "pull" solutions like 'pnpm link'.
 */
class CopyEmittedFilesPlugin {
  constructor(options) {
    validate(SCHEMA, options, {
      name: PLUGIN_NAME,
      baseDataPath: "options",
    });
    this.options = options;
  }

  /**
   * The implementation of the plugin. Runs one time once webpack starts up,
   * setting up event listeners for various webpack events.
   * @see https://webpack.js.org/contribute/writing-a-plugin/#creating-a-plugin
   */
  apply(compiler) {
    // If no destinations have been configured or if this isn't a watch-mode
    // build, this plugin should do nothing.
    // It's therefore safe to keep an instance of this plugin in the 'Plugins'
    // array, as it'll have no impact on unconfigured builds.
    if (this.options.destinations.length === 0 || !compiler.options.watch) {
      return;
    }

    const logger = compiler.getInfrastructureLogger(PLUGIN_NAME);

    // Extract the major and minor versions of this cluster-ui build.
    const pkgVersion = getPkgVersion(compiler.context);

    // Sanitize provided paths to ensure they point to a reasonable version of
    // cluster-ui.
    const destinations = this.options.destinations.map((dstOpt) => {
      const dst = detildeify(dstOpt);

      // The user provided paths to a specific cluster-ui version.
      if (dst.includes("@cockroachlabs/cluster-ui-")) {
        // Remove a possibly-trailing '/' literal.
        const dstClean = dst[dst.length - 1] === "/"
          ? dst.slice(0, dst.length - 1)
          : dst;

        return dstClean;
      }

      // If the user provided a path to a project, look for a top-level
      // node_modules/ within that directory
      const dirents = fs.readdirSync(dst, { encoding: "utf-8", withFileTypes: true });
      for (const dirent of dirents) {
        if (dirent.name === "node_modules" && dirent.isDirectory()) {
          return path.join(
            dst,
            `./node_modules/@cockroachlabs/cluster-ui-${pkgVersion.major}-${pkgVersion.minor}`,
          );
        }
      }

      const hasPnpmLock = dirents.some((dirent) => dirent.name === "pnpm-lock.yaml");
      if (hasPnpmLock) {
        logger.error(`Directory ${dst} doesn't have a node_modules directory, but does have a pnpm-lock.yaml.`);
        logger.error(`Do you need to run 'pnpm install' there?`);
        throw "missing node_modules";
      }

      logger.error(`Directory ${dst} doesn't have a node_modules directory, and does not appear to be`);
      logger.error(`a JS package.`);
      throw "unknown destination";
    });

    logger.info("Emitted files will be copied to:");
    for (const dst of destinations) {
      logger.info("  " + tildeify(dst));
    }

    const relOutputPath = path.relative(compiler.context, compiler.options.output.path);
    // Clear destination areas and recreate output directory structure in each
    // to ensure destinations all match the local output tree.
    // @see https://v4.webpack.js.org/api/compiler-hooks/#afterenvironment
    compiler.hooks.afterEnvironment.tap(PLUGIN_NAME, () => {
      logger.warn("Deleting destinations in preparation for copied files:");
      for (const dst of destinations) {
        const prettyDst = tildeify(dst);
        const stat = fs.statSync(dst);

        if (stat.isDirectory()) {
          logger.warn(`  rm -r ${prettyDst}`);
        } else {
          logger.warn(`  rm ${prettyDst}`);
        }
        fs.rmSync(dst, { recursive: stat.isDirectory() });

        logger.debug(`mkdir -p ${path.join(dst, relOutputPath)}`);
        fs.mkdirSync(path.join(dst, relOutputPath), { recursive: true });

        logger.debug(`cp package.json ${path.join(dst, "package.json")}`);
        fs.copyFileSync(
          path.join(compiler.context, "package.json"),
          path.join(dst, "package.json"),
        );
      }
    });

    // Copy files to each destination as they're emitted.
    // @see https://v4.webpack.js.org/api/compiler-hooks/#assetemitted
    compiler.hooks.assetEmitted.tapPromise(PLUGIN_NAME, (file) => {
      return Promise.all(
        destinations.map((dstBase) => {
          const prettyDst = tildeify(dstBase);
          const src = path.join(relOutputPath, file);
          const dst = path.join(dstBase, relOutputPath, file);
          logger.info(`cp ${src} ${path.join(prettyDst, relOutputPath)}`);
          return fsp.copyFile(src, dst);
        }),
      );
    });
  }
}

/**
 * Extracts the major and minor version number from the package at pkgRoot.
 * @param pkgRoot {string} - the absolute path to the directory that holds the
 *                           package's package.json
 * @returns {object} - an object containing the major (`.major`) and minor
 *                     (`.minor`) versions of the package
 */
function getPkgVersion(pkgRoot) {
  const pkgJsonStr = fs.readFileSync(
    path.join(pkgRoot, "package.json"),
    "utf-8",
  );
  const pkgJson = JSON.parse(pkgJsonStr);
  const version = semver.parse(pkgJson.version);
  return {
    major: version.major,
    minor: version.minor,
  };
}

/**
 * Replaces the user's home directory with '~' in the provided path. The
 * opposite of `detildeify`.
 * @param {string} path - the path to replace a home directory in
 * @returns {string} `path` but with the user's home directory swapped for '~'
 */
function tildeify(path) {
  return path.replace(os.homedir(), "~");
}

/**
 * Replaces '~' with the user's home directory in the provided path. The
 * opposite of `tildeify`.
 * @param {string} path - the path to replace a '~' in
 * @returns {string} `path` but with '~' swapped for the user's home directory.
 */
function detildeify(path) {
  return path.replace("~", os.homedir());
}

module.exports.CopyEmittedFilesPlugin = CopyEmittedFilesPlugin;
