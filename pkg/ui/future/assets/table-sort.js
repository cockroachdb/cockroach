document.addEventListener('alpine:init', () => {
  Alpine.data('tableSort', () => ({
    sortColumn: null,
    sortDirection: 'original',
    originalOrder: [],
    table: null,
    currentPage: 1,
    itemsPerPage: 25,

    init() {
      this.originalOrder = Array.from(this.$el.querySelectorAll('tbody tr'));
      this.table = this.$el.querySelector('tbody');
      this.renderPage();
    },

    get allRows() {
      return Array.from(this.table.querySelectorAll('tr'));
    },

    get totalPages() {
      return Math.ceil(this.allRows.length / this.itemsPerPage);
    },

    get startIndex() {
      return (this.currentPage - 1) * this.itemsPerPage;
    },

    get endIndex() {
      return Math.min(this.startIndex + this.itemsPerPage, this.allRows.length);
    },

    get paginationInfo() {
      const start = this.allRows.length === 0 ? 0 : this.startIndex + 1;
      const end = this.endIndex;
      const total = this.allRows.length;
      return `Showing ${start}-${end} of ${total} results`;
    },

    renderPage() {
      const rows = this.allRows;
      rows.forEach((row, index) => {
        if (index >= this.startIndex && index < this.endIndex) {
          row.style.display = '';
        } else {
          row.style.display = 'none';
        }
      });
    },

    nextPage() {
      if (this.currentPage < this.totalPages) {
        this.currentPage++;
        this.renderPage();
      }
    },

    prevPage() {
      if (this.currentPage > 1) {
        this.currentPage--;
        this.renderPage();
      }
    },

    goToPage(page) {
      if (page >= 1 && page <= this.totalPages) {
        this.currentPage = page;
        this.renderPage();
      }
    },

    sort(column, index) {
      if (this.sortColumn === column) {
        // Cycle through: asc → desc → original
        if (this.sortDirection === 'asc') {
          this.sortDirection = 'desc';
        } else if (this.sortDirection === 'desc') {
          this.sortDirection = 'original';
          this.sortColumn = null;
        } else {
          this.sortDirection = 'asc';
        }
      } else {
        // New column, start with ascending
        this.sortColumn = column;
        this.sortDirection = 'asc';
      }

      const tbody = this.table;

      if (this.sortDirection === 'original') {
        // Restore original order
        this.originalOrder.forEach(row => tbody.appendChild(row));
      } else {
        // Sort rows
        const rows = Array.from(tbody.querySelectorAll('tr'));
        rows.sort((a, b) => {
          const aCell = a.children[index];
          const bCell = b.children[index];
          const aValue = aCell.dataset.sort || aCell.textContent.trim();
          const bValue = bCell.dataset.sort || bCell.textContent.trim();

          // Try numeric comparison first
          const aNum = parseFloat(aValue);
          const bNum = parseFloat(bValue);

          let comparison;
          if (!isNaN(aNum) && !isNaN(bNum)) {
            comparison = aNum - bNum;
          } else {
            comparison = aValue.localeCompare(bValue);
          }

          return this.sortDirection === 'asc' ? comparison : -comparison;
        });

        rows.forEach(row => tbody.appendChild(row));
      }

      // Reset to page 1 after sorting and render the page
      this.currentPage = 1;
      this.renderPage();
    },

    getSortIndicator(column) {
      if (this.sortColumn !== column) return '↕';
      if (this.sortDirection === 'asc') return '↑';
      if (this.sortDirection === 'desc') return '↓';
      return '↕';
    }
  }));
});
