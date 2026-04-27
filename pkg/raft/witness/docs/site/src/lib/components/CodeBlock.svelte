<script lang="ts">
  // `code` is pre-highlighted HTML produced by shiki at build time, or raw
  // source text rendered verbatim if no `lang` is provided.
  let { code, sourceUrl }: { code: string; sourceUrl?: string } = $props();
</script>

<div class="code-block-wrapper">
  {@html code}
  {#if sourceUrl}
    <a class="code-source-link" href={sourceUrl} target="_blank" rel="noopener">source ↗</a>
  {/if}
</div>

<style>
  .code-block-wrapper { position: relative; }
  .code-source-link {
    position: absolute;
    top: 0.5em;
    right: 0.5em;
    font-size: 12px;
    font-family: var(--font-sans);
    color: var(--color-link);
    text-decoration: none;
    background: var(--color-background-secondary);
    padding: 0.1em 0.5em;
    border-radius: 3px;
  }
  .code-source-link:hover {
    color: var(--color-link-hover);
    text-decoration: underline;
  }
</style>
