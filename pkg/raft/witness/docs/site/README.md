# raft/witness docs site

Static documentation site for `pkg/raft/witness`. SvelteKit + mdsvex +
shiki, modeled after [cockroachlabs/basalt's docs site][basalt].

Raw markdown notes (e.g. `cost-details.md`) live one level up in
`pkg/raft/witness/docs/`. This `site/` directory is just the rendering
harness.

[basalt]: https://github.com/cockroachlabs/basalt/tree/master/docs

## Develop

```bash
cd pkg/raft/witness/docs/site
npm install
npm run dev          # http://localhost:5173, hot reload
```

## Build (static)

```bash
npm run build        # output: dist/
npm run preview      # serve dist/ locally
```

For deployment under a subpath (e.g. GitHub Pages), set `BASE_PATH`:

```bash
BASE_PATH=/<repo> npm run build
```

All in-app links use `$app/paths`'s `base` so they prepend correctly.

## Adding a new page

1. Create `src/routes/(docs)/<slug>/+page.svx`. Start with frontmatter:

   ```mdx
   ---
   title: My Page
   subtitle: Optional one-line description
   ---

   ## Section heading

   Markdown body. Code fences with a language tag are highlighted by shiki
   at build time (light + dark themes).

   ```go
   func Example() {}
   ```
   ```

2. Add an entry to `src/lib/nav/docs-nav.js`:

   ```js
   { type: 'page',    href: '/<slug>', label: 'My Page' }
   // or, for grouped pages:
   { type: 'section', href: '/<slug>', label: 'My Section' }
   { type: 'sub',     href: '/<sub>',  label: 'Sub Page' }
   ```

3. Reload `npm run dev`. Sidebar TOC and right-rail headings update on
   the fly.

## Linking to source

`sourceUrl(filePath, startLine, endLine?)` from `$lib/source/source-link`
returns `github.com/cockroachdb/cockroach/blob/<sha>/<path>#L...`. SHA is
captured by the `git-sha` Vite plugin at build time, so links remain
stable for the deployed artifact.

```svelte
<script>
  import { sourceUrl, witnessPkg } from '$lib/source/source-link';
</script>

<a href={sourceUrl(witnessPkg + '/witness.go', 42, 60)}>witness.go:42-60</a>
```

Note: Svelte expressions don't work inside markdown link syntax
`[text](url)`. Use raw `<a href={...}>` instead.

## Inlining source files

`import src from '$src/witness.go?raw'` reads any file from the witness
package as a string at build time. Use `$crdb/...` for paths elsewhere in
the cockroach checkout. Pass the string into a `<CodeBlock>` for
highlighting, or slice/format it manually.

## Diagrams

Drop Svelte components under `src/lib/components/diagrams/` and import
them into `.svx` pages. d3 is bundled.

## Layout

```
src/
  app.{html,css}                  shell + theme tokens (light + dark)
  routes/
    +layout.{svelte,js}           color mode + prerender flag
    +page.svelte                  redirects to /introduction
    (docs)/
      +layout.svelte              sidebar TOC
      <slug>/+page.svx            individual pages
  lib/
    components/                   ColorModeToggle, SidebarTOC, CodeBlock
    components/diagrams/          per-page Svelte diagrams
    layouts/ArticleLayout.svelte  default layout for .svx pages
    nav/docs-nav.js               sidebar config
    source/source-link.ts         GitHub permalink helper
    stores/color-mode.svelte.ts   light/dark mode state
```
