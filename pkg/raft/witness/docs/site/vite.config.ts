import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';
import { resolve } from 'path';
import { execSync } from 'child_process';

const siteDir = __dirname;
// Witness package root (two levels up from site: pkg/raft/witness/docs/site -> pkg/raft/witness).
const witnessDir = resolve(siteDir, '../..');
// CockroachDB repo root, five levels up from site (pkg/raft/witness/docs/site).
const repoRoot = resolve(siteDir, '../../../../..');

function gitShaPlugin() {
  return {
    name: 'git-sha',
    config() {
      let sha = 'HEAD';
      try {
        sha = execSync('git rev-parse HEAD', {
          cwd: repoRoot,
          encoding: 'utf-8'
        }).trim();
      } catch {
        // Not in a git checkout (e.g. CI tarball). Leave as 'HEAD'.
      }
      return {
        define: {
          __GIT_SHA__: JSON.stringify(sha)
        }
      };
    }
  };
}

export default defineConfig({
  plugins: [gitShaPlugin(), sveltekit()],
  build: {
    target: 'esnext'
  },
  optimizeDeps: {
    esbuildOptions: {
      target: 'esnext'
    }
  },
  resolve: {
    alias: {
      // `import x from '$src/witness.go?raw'` reads files from the witness
      // package (two levels up from docs/site/).
      $src: witnessDir,
      // `import x from '$docs/landing.md?raw'` reads sibling markdown notes
      // from pkg/raft/witness/docs/.
      $docs: resolve(siteDir, '..'),
      // `import x from '$crdb/pkg/raft/.../foo.go?raw'` reads from anywhere
      // in the cockroach checkout.
      $crdb: repoRoot
    }
  },
  server: {
    fs: {
      allow: [witnessDir, repoRoot]
    }
  }
});
