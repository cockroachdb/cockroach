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
