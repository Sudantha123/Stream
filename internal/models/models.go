package models

import (
	"sync"
	"time"
)

type JobStatus string

const (
	StatusPending      JobStatus = "pending"
	StatusDownloading  JobStatus = "downloading"
	StatusTranscoding  JobStatus = "transcoding"
	StatusThumbnailing JobStatus = "thumbnailing"
	StatusDone         JobStatus = "done"
	StatusFailed       JobStatus = "failed"
	StatusCancelled    JobStatus = "cancelled"
)

type Job struct {
	ID           string    `json:"id"`
	URL          string    `json:"url"`
	Title        string    `json:"title"`
	OriginalName string    `json:"original_name"` // original filename from URL
	Status       JobStatus `json:"status"`

	// Download progress
	DownloadedBytes int64   `json:"downloaded_bytes"`
	TotalBytes      int64   `json:"total_bytes"`
	DownloadSpeed   float64 `json:"download_speed"`
	DownloadETA     float64 `json:"download_eta"`
	DownloadPct     float64 `json:"download_pct"`

	// fMP4 conversion progress
	TranscodePct float64 `json:"transcode_pct"`

	Error     string    `json:"error,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`

	CancelFunc func() `json:"-"`
	mu         sync.Mutex
}

func (j *Job) Update(fn func(*Job)) {
	j.mu.Lock()
	defer j.mu.Unlock()
	fn(j)
	j.UpdatedAt = time.Now()
}

type Video struct {
	ID           string    `json:"id"`
	Title        string    `json:"title"`
	OriginalName string    `json:"original_name"` // filename for streaming URL
	Thumbnail    string    `json:"thumbnail"`
	Duration     float64   `json:"duration"`
	SizeBytes    int64     `json:"size_bytes"`
	CreatedAt    time.Time `json:"created_at"`
	FMP4Path     string    `json:"fmp4_path"` // e.g. /videos/Mr. X (2026).mp4
}

type AddJobRequest struct {
	URL   string `json:"url"`
	Title string `json:"title"`
}

type WSMessage struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}
