package downloader

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"streamvault/internal/models"
)

const bufSize = 512 * 1024 // 512KB buffer for speed

type progressWriter struct {
	job       *models.Job
	written   int64
	startTime time.Time
	lastTime  time.Time
	lastBytes int64
	broadcast func(job *models.Job)
}

func (pw *progressWriter) Write(p []byte) (int, error) {
	n := len(p)
	pw.written += int64(n)

	now := time.Now()
	elapsed := now.Sub(pw.lastTime).Seconds()
	if elapsed >= 0.5 {
		bytesDelta := pw.written - pw.lastBytes
		speed := float64(bytesDelta) / elapsed

		pw.job.Update(func(j *models.Job) {
			j.DownloadedBytes = pw.written
			j.DownloadSpeed = speed
			if j.TotalBytes > 0 && speed > 0 {
				remaining := float64(j.TotalBytes-pw.written)
				j.DownloadETA = remaining / speed
				j.DownloadPct = float64(pw.written) / float64(j.TotalBytes) * 100
			}
		})

		pw.lastTime = now
		pw.lastBytes = pw.written
		pw.broadcast(pw.job)
	}
	return n, nil
}

func Download(ctx context.Context, job *models.Job, destPath string, broadcast func(*models.Job)) error {
	req, err := http.NewRequestWithContext(ctx, "GET", job.URL, nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("User-Agent", "StreamVault/1.0")

	client := &http.Client{
		Transport: &http.Transport{
			DisableCompression:  true,
			MaxIdleConnsPerHost: 4,
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("server returned %d", resp.StatusCode)
	}

	job.Update(func(j *models.Job) {
		j.TotalBytes = resp.ContentLength
		j.Status = models.StatusDownloading
	})
	broadcast(job)

	f, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("creating file: %w", err)
	}
	defer f.Close()

	pw := &progressWriter{
		job:       job,
		startTime: time.Now(),
		lastTime:  time.Now(),
		broadcast: broadcast,
	}

	buf := make([]byte, bufSize)
	_, err = io.CopyBuffer(io.MultiWriter(f, pw), resp.Body, buf)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("downloading: %w", err)
	}

	job.Update(func(j *models.Job) {
		j.DownloadPct = 100
		j.DownloadedBytes = pw.written
		j.DownloadETA = 0
	})
	broadcast(job)
	return nil
}
