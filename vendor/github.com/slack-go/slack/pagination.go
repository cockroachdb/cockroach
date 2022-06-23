package slack

// Paging contains paging information
type Paging struct {
	Count int `json:"count"`
	Total int `json:"total"`
	Page  int `json:"page"`
	Pages int `json:"pages"`
}

// Pagination contains pagination information
// This is different from Paging in that it contains additional details
type Pagination struct {
	TotalCount int `json:"total_count"`
	Page       int `json:"page"`
	PerPage    int `json:"per_page"`
	PageCount  int `json:"page_count"`
	First      int `json:"first"`
	Last       int `json:"last"`
}
