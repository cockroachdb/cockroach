/**
 * Auto-refresh for development with ibazel
 * Automatically refreshes the page when the server restarts
 */
(function() {
  'use strict';

  const config = {
    checkInterval: 1000,     // Check every second when server is up
    retryInterval: 250,      // Check every 250ms when server is down
    endpoint: '/future/login',  // Endpoint to check
    showIndicator: true,
    debug: true,
    // Optional: Custom validation function
    // Set window.AUTO_REFRESH_VALIDATOR to override
    customValidator: window.AUTO_REFRESH_VALIDATOR || null
  };

  let isServerDown = false;
  let checkTimer = null;
  let indicator = null;

  function createIndicator() {
    if (!config.showIndicator) return;

    indicator = document.createElement('div');
    indicator.style.cssText = `
      position: fixed;
      bottom: 20px;
      right: 20px;
      padding: 10px 15px;
      background: rgba(76, 175, 80, 0.9);
      color: white;
      border-radius: 6px;
      font-family: monospace;
      font-size: 13px;
      font-weight: bold;
      z-index: 10000;
      transition: all 0.3s ease;
      box-shadow: 0 2px 10px rgba(0,0,0,0.2);
    `;
    indicator.textContent = '✅ Auto-refresh active';
    document.body.appendChild(indicator);

    // Fade out after 2 seconds
    setTimeout(() => {
      if (indicator && !isServerDown) {
        indicator.style.opacity = '0';
      }
    }, 2000);
  }

  function updateIndicator(status, message) {
    if (!indicator) return;

    indicator.style.opacity = '1';
    indicator.textContent = message;

    const colors = {
      down: 'rgba(244, 67, 54, 0.9)',
      checking: 'rgba(255, 152, 0, 0.9)',
      ready: 'rgba(76, 175, 80, 0.9)'
    };

    indicator.style.background = colors[status] || colors.ready;
  }

  function log(msg) {
    if (config.debug) {
      console.log(`[Auto-Refresh] ${msg}`);
    }
  }

  function checkServer() {
    fetch(config.endpoint, {
      method: 'GET',
      cache: 'no-cache',
      headers: {
        'Accept': 'text/html'
      }
    })
    .then(response => {
      // Check if we got a valid response with actual content
      if (response.ok && response.status === 200) {
        return response.text();
      }
      throw new Error(`Got status ${response.status}`);
    })
    .then(html => {
      // Verify we got real HTML content, not an error page
      let isValidHTML;

      if (config.customValidator) {
        // Use custom validation function if provided
        isValidHTML = config.customValidator(html);
      } else {
        // Default validation: check for HTML structure and no error messages
        isValidHTML = html &&
                      html.length > 100 &&  // Has substantial content
                      (html.includes('<!DOCTYPE') || html.includes('<html')) &&  // Is HTML
                      !html.includes('404 Not Found') &&  // Not a 404 page
                      !html.includes('Cannot GET');  // Not an error message
      }

      if (isServerDown && isValidHTML) {
        // Server is back with valid content!
        log('Server back online with valid content! Refreshing in 500ms...');
        updateIndicator('ready', '🚀 Reloading...');

        setTimeout(() => {
          window.location.reload();
        }, 500);  // Give server more time to stabilize
      } else if (!isServerDown) {
        // Server still up, continue monitoring
        scheduleCheck(config.checkInterval);
      } else if (isServerDown && !isValidHTML) {
        // Server responded but content not ready yet
        log('Server responding but content not ready (got ' + (html ? html.length : 0) + ' bytes)...');
        updateIndicator('checking', '⏳ Waiting for full startup...');
        scheduleCheck(config.retryInterval);
      }
    })
    .catch(error => {
      if (!isServerDown) {
        // Server just went down
        isServerDown = true;
        log('Server down, waiting for restart...');
        updateIndicator('down', '🔄 Server restarting...');
      } else {
        // Still down or not fully ready
        updateIndicator('checking', '⏳ Waiting for server...');
      }
      // Check more frequently when down
      scheduleCheck(config.retryInterval);
    });
  }

  function scheduleCheck(interval) {
    if (checkTimer) clearTimeout(checkTimer);
    checkTimer = setTimeout(checkServer, interval);
  }

  // Initialize
  function init() {
    // Only run on localhost/dev
    const isDev = ['localhost', '127.0.0.1'].includes(window.location.hostname);
    if (!isDev) return;

    log('Initializing development auto-refresh');

    // Wait for DOM
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', createIndicator);
    } else {
      createIndicator();
    }

    // Start checking
    scheduleCheck(config.checkInterval);

    // Cleanup
    window.addEventListener('beforeunload', () => {
      if (checkTimer) clearTimeout(checkTimer);
    });
  }

  init();
})();
