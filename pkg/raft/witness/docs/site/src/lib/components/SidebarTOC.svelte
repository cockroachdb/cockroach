<script>
  import { page } from '$app/stores';
  import { afterNavigate } from '$app/navigation';
  import { onMount } from 'svelte';

  let { home, crossLinks = [], pages } = $props();

  let headings = $state([]);
  let activeId = $state('');
  let tocNav = $state(null);
  let observer = $state(null);

  let currentPage = $derived($page.url.pathname.slice(1));

  $effect(() => {
    if (activeId && tocNav) {
      const activeElement = tocNav.querySelector(`.toc-link.active`);
      if (activeElement) {
        activeElement.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      }
    }
  });

  function extractHeadings() {
    if (observer) {
      observer.disconnect();
      observer = null;
    }

    const article = document.querySelector('.article-content');
    if (!article) {
      headings = [];
      return;
    }

    const elements = article.querySelectorAll('h2, h3');
    const items = [];

    elements.forEach((el, index) => {
      if (!el.id) {
        el.id = `heading-${index}`;
      }
      items.push({
        id: el.id,
        text: el.textContent,
        level: el.tagName === 'H2' ? 2 : 3
      });
    });

    headings = items;
    activeId = '';

    observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting) {
            activeId = entry.target.id;
          }
        });
      },
      {
        rootMargin: '-80px 0px -80% 0px',
        threshold: 0
      }
    );

    elements.forEach((el) => observer.observe(el));
  }

  onMount(() => {
    extractHeadings();
    return () => {
      if (observer) observer.disconnect();
    };
  });

  afterNavigate(() => {
    extractHeadings();
  });

  function pageClass(type) {
    if (type === 'sub') return 'toc-item sub-item';
    if (type === 'section') return 'toc-item section-header';
    return 'toc-item';
  }
</script>

<nav class="toc" bind:this={tocNav}>
  <div class="toc-section">
    <div class="toc-title">
      <a href={home.href} class="toc-home-link">{home.label}</a>
      {#if crossLinks.length > 0}
        <span class="toc-cross-links">
          {#each crossLinks as link, i}
            {#if i > 0}<span class="toc-cross-sep">·</span>{/if}
            <a href={link.href} class="toc-nav-link">{link.label}</a>
          {/each}
        </span>
      {/if}
    </div>
    <ul class="toc-list page-nav">
      {#each pages as p}
        <li class={pageClass(p.type)}>
          <a href={p.href} class="toc-link page-link" class:active={currentPage === p.href.slice(1)}>{p.label}</a>
        </li>
      {/each}
    </ul>
  </div>

  {#if headings.length > 0}
  <div class="toc-section">
    <div class="toc-title">Contents</div>
    <ul class="toc-list">
      {#each headings as heading}
        <li class="toc-item" class:level-3={heading.level === 3}>
          <a
            href="#{heading.id}"
            class="toc-link"
            class:active={activeId === heading.id}
          >
            {heading.text}
          </a>
        </li>
      {/each}
    </ul>
  </div>
  {/if}
</nav>

<style>
  .toc {
    position: fixed;
    top: 40px;
    left: max(20px, calc((100vw - 1200px) / 2 - 300px));
    width: 280px;
    max-height: calc(100vh - 80px);
    overflow-y: auto;
    font-family: var(--font-sans);
    font-size: 13px;
  }

  .toc-section { margin-bottom: 1.5rem; }

  .toc-title {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    font-weight: 600;
    color: var(--color-text);
    margin-bottom: 0.75rem;
    padding-bottom: 0.5rem;
    border-bottom: 1px solid var(--color-border);
  }

  .toc-cross-links { display: inline-flex; align-items: baseline; gap: 0.35rem; }
  .toc-cross-sep { color: var(--color-text-muted); font-weight: 400; font-size: 11px; }

  .toc-nav-link {
    font-weight: 400;
    font-size: 12px;
    color: var(--color-toc-nav);
    text-decoration: none;
  }
  .toc-nav-link:hover { color: var(--color-link); }

  .toc-home-link { color: inherit; text-decoration: none; }
  .toc-home-link:hover { color: var(--color-link); text-decoration: none; }

  .page-link { text-decoration: none; }

  .toc-list { list-style: none; padding: 0; margin: 0; }
  .toc-item { margin: 0; padding: 0; }

  .toc-item.sub-item { padding-left: 0.75rem; }
  .toc-item.sub-item .toc-link {
    color: var(--color-toc-link-secondary);
    font-weight: 400;
  }
  .toc-item.sub-item .toc-link:hover { color: var(--color-toc-link-secondary-hover); }
  .toc-item.sub-item .toc-link.active { color: var(--color-link); font-weight: 500; }

  .toc-item.level-3 { padding-left: 0.75rem; }
  .toc-item.level-3 .toc-link {
    color: var(--color-toc-link-secondary);
    font-weight: 400;
  }
  .toc-item.level-3 .toc-link:hover { color: var(--color-toc-link-secondary-hover); }
  .toc-item.level-3 .toc-link.active { color: var(--color-link); font-weight: 500; }

  .toc-link {
    font-family: inherit;
    font-size: inherit;
    font-weight: 600;
    display: block;
    width: 100%;
    padding: 0.15rem 0;
    text-align: left;
    border-left: 2px solid transparent;
    padding-left: 0.75rem;
    color: var(--color-toc-link);
    text-decoration: none;
    transition: color 0.15s, border-color 0.15s;
    line-height: 1.3;
  }

  .toc-link:hover { color: var(--color-toc-link-hover); text-decoration: none; }

  .toc-link.active {
    color: var(--color-link);
    border-left-color: var(--color-link);
    font-weight: 600;
  }

  @media (max-width: 1200px) {
    .toc { display: none; }
  }
</style>
