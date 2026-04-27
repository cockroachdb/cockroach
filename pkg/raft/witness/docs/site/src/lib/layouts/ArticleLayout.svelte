<script>
  let { title = '', subtitle = '', description = '', children } = $props();
  const sub = subtitle || description;
</script>

<main class="article">
  {#if title}
    <header class="article-header">
      <h1 class="article-title">{title}</h1>
      {#if sub}
        <p class="article-subtitle">{sub}</p>
      {/if}
    </header>
  {/if}

  <article class="article-content">
    {@render children()}
  </article>
</main>

<style>
  .article-content { position: relative; }
  .article-content :global(h2) { margin-top: 1.75rem; }
  .article-content :global(figure) { margin: 2rem 0; }
  .article-content :global(table) { width: 100%; }
</style>
