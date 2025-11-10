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
        interval: 'Last 10m',
        startTime: '',
        endTime: '',
        startTimeFull: null,
        endTimeFull: null,
        timezone: 'America/New_York',
        pollingInterval: null,

        init() {
          this.updateTimeRangeFromInterval();
          this.startPolling();
        },

        handleIntervalChange() {
          if (this.interval !== 'Custom') {
            this.updateTimeRangeFromInterval();
            this.startPolling();
          } else {
            this.stopPolling();
          }
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
          const now = new Date();
          const end = now;
          let start = new Date();

          switch (this.interval) {
            case 'Last 10m':
              start.setMinutes(now.getMinutes() - 10);
              break;
            case 'Last 30m':
              start.setMinutes(now.getMinutes() - 30);
              break;
            case 'Last 1h':
              start.setHours(now.getHours() - 1);
              break;
            default:
              return;
          }

          this.startTime = this.formatDateTimeLocal(start);
          this.endTime = this.formatDateTimeLocal(end);
          this.startTimeFull = start
          this.endTimeFull = end
        },

        formatDateTimeLocal(date) {
          const year = date.getFullYear();
          const month = String(date.getMonth() + 1).padStart(2, '0');
          const day = String(date.getDate()).padStart(2, '0');
          const hours = String(date.getHours()).padStart(2, '0');
          const minutes = String(date.getMinutes()).padStart(2, '0');
          return `${year}-${month}-${day}T${hours}:${minutes}`;
        },

        dateTimeLocalToNanos(date) {
          return BigInt(date.getTime()) * 1_000_000n;
        },

        getStartNanos() {
          return this.dateTimeLocalToNanos(this.startTimeFull).toString();
        },

        getEndNanos() {
          return this.dateTimeLocalToNanos(this.endTimeFull).toString();
        }
      });

      // Initialize the time range
      Alpine.store('timeRange').init();
    });
