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
	ID            string    `json:"id"`
	URL           string    `json:"url"`
	Title         string    `json:"title"`
	Status        JobStatus `json:"status"`
	SegmentLength int       `json:"segment_length"` // seconds

	// Download progress
	DownloadedBytes int64   `json:"downloaded_bytes"`
	TotalBytes      int64   `json:"total_bytes"`
	DownloadSpeed   float64 `json:"download_speed"`  // bytes/sec
	DownloadETA     float64 `json:"download_eta"`    // seconds
	DownloadPct     float64 `json:"download_pct"`

	// Transcode progress
	TranscodeSegments int `json:"transcode_segments"`
	TranscodeDone     int `json:"transcode_done"`
	TranscodePct      float64 `json:"transcode_pct"`

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
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	Thumbnail string    `json:"thumbnail"`
	Duration  float64   `json:"duration"`
	SizeBytes int64     `json:"size_bytes"`
	CreatedAt time.Time `json:"created_at"`
	M3U8Path  string    `json:"m3u8_path"`
}

type AddJobRequest struct {
	URL           string `json:"url"`
	Title         string `json:"title"`
	SegmentLength int    `json:"segment_length"`
}

type WSMessage struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}
