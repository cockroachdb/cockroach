// GitHub permalink helpers. The git SHA is captured at build time by the
// `git-sha` Vite plugin in vite.config.ts and substituted as a literal at
// build time, so links remain stable for the deployed artifact.

declare const __GIT_SHA__: string;

const ORG = 'cockroachdb';
const REPO = 'cockroach';

/** Permalink to a single line or line range in the cockroach repo. */
export function sourceUrl(filePath: string, startLine: number, endLine?: number): string {
  const range = endLine && endLine !== startLine ? `L${startLine}-L${endLine}` : `L${startLine}`;
  return `https://github.com/${ORG}/${REPO}/blob/${__GIT_SHA__}/${filePath}#${range}`;
}

/** Permalink to a file (no line anchor). */
export function fileUrl(filePath: string): string {
  return `https://github.com/${ORG}/${REPO}/blob/${__GIT_SHA__}/${filePath}`;
}

/** Path of the witness package, for use as a base when building file links. */
export const witnessPkg = 'pkg/raft/witness';
