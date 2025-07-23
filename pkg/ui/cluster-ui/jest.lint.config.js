module.exports = {
  displayName: "lint",
  runner: "jest-runner-eslint",
  testMatch: ["<rootDir>/src/**/*.{tsx,ts,js}"],
  modulePathIgnorePatterns: ["<rootDir>\/src\/.*\/.*\\.stories\\.tsx?$"],
};
