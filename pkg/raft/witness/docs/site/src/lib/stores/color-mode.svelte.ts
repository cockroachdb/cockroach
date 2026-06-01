// Reactive dark-mode state using Svelte 5 runes.

export type ColorMode = 'light' | 'dark';

const STORAGE_KEY = 'color-mode';

let mode = $state<ColorMode>('light');

export function applyColorModeToDOM(): void {
  if (typeof document === 'undefined') return;
  document.documentElement.classList.toggle('dark', mode === 'dark');
}

export function getColorMode(): ColorMode {
  return mode;
}

export function toggleColorMode(): void {
  mode = mode === 'dark' ? 'light' : 'dark';
  try {
    localStorage.setItem(STORAGE_KEY, mode);
  } catch {
    // localStorage unavailable
  }
  applyColorModeToDOM();
}

export function setColorMode(m: ColorMode): void {
  mode = m;
  applyColorModeToDOM();
}

export function initColorMode(): void {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored === 'dark' || stored === 'light') {
      mode = stored;
      applyColorModeToDOM();
      return;
    }
  } catch {
    // localStorage unavailable
  }

  if (typeof window !== 'undefined' && window.matchMedia?.('(prefers-color-scheme: dark)').matches) {
    mode = 'dark';
  } else {
    mode = 'light';
  }
  applyColorModeToDOM();
}
