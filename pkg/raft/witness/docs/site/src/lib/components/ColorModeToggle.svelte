<script lang="ts">
  import { getColorMode, toggleColorMode } from '$lib/stores/color-mode.svelte';

  let dark = $derived(getColorMode() === 'dark');
</script>

<button
  class="color-mode-toggle"
  onclick={toggleColorMode}
  title={dark ? 'Switch to light mode' : 'Switch to dark mode'}
  aria-label={dark ? 'Switch to light mode' : 'Switch to dark mode'}
>
  {#if dark}
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
      <circle cx="12" cy="12" r="5" />
      <line x1="12" y1="1" x2="12" y2="3" />
      <line x1="12" y1="21" x2="12" y2="23" />
      <line x1="4.22" y1="4.22" x2="5.64" y2="5.64" />
      <line x1="18.36" y1="18.36" x2="19.78" y2="19.78" />
      <line x1="1" y1="12" x2="3" y2="12" />
      <line x1="21" y1="12" x2="23" y2="12" />
      <line x1="4.22" y1="19.78" x2="5.64" y2="18.36" />
      <line x1="18.36" y1="5.64" x2="19.78" y2="4.22" />
    </svg>
  {:else}
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
      <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
    </svg>
  {/if}
</button>

<style>
  .color-mode-toggle {
    position: fixed;
    top: 16px;
    right: 16px;
    z-index: 100;
    display: flex;
    align-items: center;
    justify-content: center;
    width: 32px;
    height: 32px;
    padding: 0;
    border: 1px solid var(--color-border);
    border-radius: 6px;
    background: var(--color-background);
    color: var(--color-text-muted);
    cursor: pointer;
    transition: color 0.15s, border-color 0.15s, background 0.15s;
  }

  .color-mode-toggle:hover {
    color: var(--color-text);
    border-color: var(--color-text-muted);
  }

  @media (max-width: 768px) {
    .color-mode-toggle {
      top: 12px;
      right: 12px;
    }
  }
</style>
