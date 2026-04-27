<script>
  import SidebarTOC from '$lib/components/SidebarTOC.svelte';
  import { home, crossLinks, pages } from '$lib/nav/docs-nav.js';

  let { children } = $props();
</script>

<SidebarTOC {home} {crossLinks} {pages} />

{@render children()}
