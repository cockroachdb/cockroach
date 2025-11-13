document.addEventListener('alpine:init', () => {
  Alpine.store('timeRangeSQL', {
    interval: '1h',
    _startTime: null,
    _endTime: null,
    timezone: 'America/New_York',
    pollingInterval: null,

    // For datetime-local inputs - format Date to string on get, parse string to Date on set
    get startTime() {
      return this._startTime ? this.formatDateTimeLocal(this._startTime) : '';
    },

    set startTime(value) {
      if (value) {
        this._startTime = new Date(value);
      }
    },

    get endTime() {
      return this._endTime ? this.formatDateTimeLocal(this._endTime) : '';
    },

    set endTime(value) {
      if (value) {
        this._endTime = new Date(value);
      }
    },

    // For hidden inputs - return seconds from Date objects
    get startSeconds() {
      return this._startTime ? Math.floor(this._startTime.getTime() / 1000) : '';
    },

    set startSeconds(value) {
      if (value) {
        this._startTime = new Date(parseInt(value) * 1000);
      }
    },

    get endSeconds() {
      return this._endTime ? Math.floor(this._endTime.getTime() / 1000) : '';
    },

    set endSeconds(value) {
      if (value) {
        this._endTime = new Date(parseInt(value) * 1000);
      }
    },

    init() {
      // Read initial interval from the select element
      const intervalSelect = document.querySelector('#interval');
      if (intervalSelect?.value) {
        this.interval = intervalSelect.value;
      }

      // Read initial values from hidden inputs if they exist
      const startInput = document.querySelector('input[name="start"]');
      const endInput = document.querySelector('input[name="end"]');

      if (startInput?.value && endInput?.value) {
        this.startSeconds = startInput.value;
        this.endSeconds = endInput.value;
      } else {
        this.updateTimeRangeFromInterval();
      }
    },

    handleIntervalChange() {
      if (this.interval !== 'Custom') {
        this.updateTimeRangeFromInterval();
      }
    },

    updateTimeRangeFromInterval() {
      const now = new Date();
      let start = new Date();

      switch (this.interval) {
        case '1h':
          start.setHours(now.getHours() - 1);
          break;
        case '6h':
          start.setHours(now.getHours() - 6);
          break;
        case '1d':
          start.setDate(now.getDate() - 1);
          break;
        case '2d':
          start.setDate(now.getDate() - 2);
          break;
        case '3d':
          start.setDate(now.getDate() - 3);
          break;
        case '1w':
          start.setDate(now.getDate() - 7);
          break;
        case '2w':
          start.setDate(now.getDate() - 14);
          break;
        case '1m':
          start.setMonth(now.getMonth() - 1);
          break;
        default:
          return;
      }

      this._startTime = start;
      this._endTime = now;
    },

    formatDateTimeLocal(date) {
      const year = date.getFullYear();
      const month = String(date.getMonth() + 1).padStart(2, '0');
      const day = String(date.getDate()).padStart(2, '0');
      const hours = String(date.getHours()).padStart(2, '0');
      const minutes = String(date.getMinutes()).padStart(2, '0');
      return `${year}-${month}-${day}T${hours}:${minutes}`;
    }
  });

  // Initialize the time range
  Alpine.store('timeRangeSQL').init();
});

document.addEventListener('alpine:init', () => {
      Alpine.store('timeRange', {
        _interval: '10m',
        startUTCSeconds: 0,
        endUTCSeconds: 0,
        timezone: 'UTC',
        pollingInterval: null,

        init() {
          this.updateTimeRangeFromInterval();
          this.startPolling();
        },

        set interval(newInterval) {
          this._interval = newInterval
          if (this.interval !== 'Custom') {
            this.updateTimeRangeFromInterval();
            this.startPolling();
          } else {
            this.stopPolling();
          }
        },

        get interval() {
          return this._interval
        },

        startPolling() {
          this.stopPolling(); // Clear any existing interval
          if (this.interval !== 'Custom') {
            this.pollingInterval = setInterval(() => {
              this.updateTimeRangeFromInterval();
            }, 10000); // 10 seconds
          }
        },

        stopPolling() {
          if (this.pollingInterval) {
            clearInterval(this.pollingInterval);
            this.pollingInterval = null;
          }
        },

        updateTimeRangeFromInterval() {
          const end = Math.floor(new Date().getTime() / 1000)
          if (this.interval === '10m') {
            this.startUTCSeconds = end - 10 * 60
          }
          if (this.interval === '1h') {
            this.startUTCSeconds = end - 60 * 60
          }
          if (this.interval === '6h') {
            this.startUTCSeconds = end - 6 * 60 * 60
          }
          if (this.interval === '1d') {
            this.startUTCSeconds = end - 24 * 60 * 60
          }
          if (this.interval === '2d') {
            this.startUTCSeconds = end - 2 * 24 * 60 * 60
          }
          if (this.interval === '3d') {
            this.startUTCSeconds = end - 3 * 24 * 60 * 60
          }
          if (this.interval === '1w') {
            this.startUTCSeconds = end - 7 * 24 * 60 * 60
          }
          if (this.interval === '2w') {
            this.startUTCSeconds = end - 14 * 24 * 60 * 60
          }
          if (this.interval === '1m') {
            this.startUTCSeconds = end - 30 * 24 * 60 * 60
          }
          if (this.interval === '2m') {
            this.startUTCSeconds = end - 60 * 24 * 60 * 60
          }

          this.endUTCSeconds = end
        },

        dateTimeLocalToNanos(date) {
          return BigInt(date.getTime()) * 1_000_000n;
        },

        getStartNanos() {
          return (BigInt(this.startUTCSeconds * 1000) * 1_000_000n).toString();
        },

        getEndNanos() {
          return (BigInt(this.endUTCSeconds * 1000) * 1_000_000n).toString();
        },

        set startFormatted(date) {
          this.startUTCSeconds = Math.floor(new Date(date + 'Z').getTime() / 1000);
        },

        get startFormatted() {
          return new Date(this.startUTCSeconds * 1000).toISOString().slice(0, 16);
        },

        set endFormatted(date) {
          this.endUTCSeconds = Math.floor(new Date(date + 'Z').getTime() / 1000);
        },

        get endFormatted() {
          return new Date(this.endUTCSeconds * 1000).toISOString().slice(0, 16);
        },
      });

      // Initialize the time range
      Alpine.store('timeRange').init();
    });
