<script>
  import '../app.css';
  import { onMount, tick } from 'svelte';
  import { initColorMode, applyColorModeToDOM, getColorMode } from '$lib/stores/color-mode.svelte';
  import ColorModeToggle from '$lib/components/ColorModeToggle.svelte';

  let { children } = $props();

  onMount(() => {
    initColorMode();
    tick().then(() => {
      document.getElementById('dark-mode-preload')?.remove();
    });
  });

  $effect(() => {
    getColorMode();
    applyColorModeToDOM();
  });
</script>

<svelte:head>
  <title>Raft Witness</title>
</svelte:head>

<ColorModeToggle />

{@render children()}
