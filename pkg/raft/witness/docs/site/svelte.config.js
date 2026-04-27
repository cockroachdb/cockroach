import { join } from 'path';
import { fileURLToPath } from 'url';
import adapter from '@sveltejs/adapter-static';
import { vitePreprocess } from '@sveltejs/vite-plugin-svelte';
import { mdsvex, escapeSvelte } from 'mdsvex';
import { createHighlighter } from 'shiki';
import remarkGfm from 'remark-gfm';
import rehypeSlug from 'rehype-slug';

const __dirname = fileURLToPath(new URL('.', import.meta.url));

const langs = ['go', 'bash', 'javascript', 'typescript', 'json', 'protobuf', 'sql', 'diff'];

const highlighter = await createHighlighter({
  themes: ['github-light', 'github-dark'],
  langs
});

// Base path for hosting under a subpath (e.g. GitHub Pages at
// /<repo>/<subpath>). Set BASE_PATH=/cockroach when deploying to
// cockroachdb.github.io/cockroach. Empty string for local dev.
const basePath = process.env.BASE_PATH ?? '';

/** @type {import('@sveltejs/kit').Config} */
export default {
  extensions: ['.svelte', '.svx'],
  preprocess: [
    vitePreprocess(),
    mdsvex({
      extensions: ['.svx'],
      layout: {
        _: join(__dirname, './src/lib/layouts/ArticleLayout.svelte')
      },
      remarkPlugins: [remarkGfm],
      rehypePlugins: [rehypeSlug],
      highlight: {
        highlighter: (code, lang) => {
          const html = escapeSvelte(
            highlighter.codeToHtml(code, {
              lang: lang || 'text',
              themes: { light: 'github-light', dark: 'github-dark' },
              defaultColor: false
            })
          );
          return `{@html \`${html}\`}`;
        }
      }
    })
  ],
  kit: {
    adapter: adapter({
      pages: 'dist',
      assets: 'dist',
      fallback: undefined,
      precompress: false,
      strict: true
    }),
    paths: {
      base: basePath
    },
    prerender: {
      handleHttpError: 'warn',
      handleMissingId: 'warn'
    }
  }
};
