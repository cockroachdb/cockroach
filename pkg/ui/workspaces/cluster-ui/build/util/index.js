// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

const fs = require("fs");
const os = require("os");
const path = require("path");
const semver = require("semver");

/**
 * Cleans up destination paths to ensure they point to valid directories,
 * automatically adding node_modules/@cockroachlabs/cluster-ui-XX-Y (for the
 * current XX-Y in this package's package.json) if a destination isn't specific 
 * to a single CRDB version.
 * @param {string[]} destinations - an array of paths
 * @param {Object} [logger=console] - the logger to use when reporting messages (defaults to `console`)
 * @returns {string[]} `destinations`, but cleaned up
 */
function cleanDestinationPaths(destinations, logger=console) {
  // Extract the major and minor versions of this cluster-ui build.
  const pkgVersion = getPkgVersion();

  return destinations.map(dstOpt => {
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
}

/**
 * Extracts the major and minor version number from the cluster-ui package.
 * @returns {object} - an object containing the major (`.major`) and minor
 *                     (`.minor`) versions of the package
 */
function getPkgVersion() {
  const pkgJsonStr = fs.readFileSync(
    path.join(__dirname, "../../package.json"),
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


module.exports = {
  tildeify,
  detildeify,
  cleanDestinationPaths,
}
