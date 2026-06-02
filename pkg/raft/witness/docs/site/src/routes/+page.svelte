<script>
  import { goto } from '$app/navigation';
  import { base } from '$app/paths';
  import { onMount } from 'svelte';

  onMount(() => {
    goto(`${base}/introduction`, { replaceState: true });
  });
</script>

<noscript>
  <p style="padding: 2rem; text-align: center;">
    <a href="{base}/introduction">Introduction</a>
  </p>
</noscript>
