module.exports = {
  displayName: "test",
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
  setupFilesAfterEnv: ["jest-enzyme"],
  testRegex: "(/__tests__/.*|(\\.|/)(test|spec))\\.tsx?$",
  transform: {
    "^.+\\.tsx?$": "ts-jest",
    "^.+\\.jsx?$": "babel-jest"
  },
  transformIgnorePatterns: [
    "node_modules/(?!(@cockroachlabs/crdb-protobuf-client)/)"
  ]
};
