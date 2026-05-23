package transcoder

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"streamvault/internal/models"
)

var timeRe = regexp.MustCompile(`time=(\d+):(\d+):(\d+)\.(\d+)`)

func GetDuration(inputPath string) (float64, error) {
	out, err := exec.Command("ffprobe",
		"-v", "error",
		"-show_entries", "format=duration",
		"-of", "default=noprint_wrappers=1:nokey=1",
		inputPath,
	).Output()
	if err != nil {
		return 0, err
	}
	s := strings.TrimSpace(string(out))
	return strconv.ParseFloat(s, 64)
}

// ConvertToFMP4 converts input video to Fragmented MP4 (fMP4) with a global SIDX index
// using: ffmpeg -i input -movflags frag_keyframe+dash+global_sidx -codec copy output
// Output file is saved with the same name as input (but in outputDir).
func ConvertToFMP4(ctx context.Context, job *models.Job, inputPath, outputDir string, broadcast func(*models.Job)) (string, error) {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return "", fmt.Errorf("mkdir: %w", err)
	}

	// Use the original filename (same name as input)
	origName := filepath.Base(inputPath)
	// Strip .tmp extension if present (downloaded tmp file)
	if strings.HasSuffix(origName, ".tmp") {
		origName = job.OriginalName
	}
	if origName == "" {
		origName = job.ID + ".mp4"
	}

	outputPath := filepath.Join(outputDir, origName)

	duration, err := GetDuration(inputPath)
	if err != nil {
		duration = 0
	}

	job.Update(func(j *models.Job) {
		j.Status = models.StatusTranscoding
		j.TranscodePct = 0
	})
	broadcast(job)

	args := []string{
		"-hide_banner",
		"-loglevel", "info",
		"-progress", "pipe:2",
		"-i", inputPath,
		"-movflags", "frag_keyframe+dash+global_sidx",
		"-codec", "copy",
		outputPath,
	}

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return "", fmt.Errorf("stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("ffmpeg start: %w", err)
	}

	// Parse progress from ffmpeg -progress pipe:2
	go func() {
		scanner := bufio.NewScanner(stderr)
		var outTimeMs float64
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "out_time_ms=") {
				val, err := strconv.ParseFloat(strings.TrimPrefix(line, "out_time_ms="), 64)
				if err == nil {
					outTimeMs = val / 1e6
				}
			}
			if m := timeRe.FindStringSubmatch(line); m != nil {
				h, _ := strconv.ParseFloat(m[1], 64)
				min, _ := strconv.ParseFloat(m[2], 64)
				sec, _ := strconv.ParseFloat(m[3], 64)
				outTimeMs = h*3600 + min*60 + sec
			}

			if outTimeMs > 0 && duration > 0 {
				pct := (outTimeMs / duration) * 100
				job.Update(func(j *models.Job) {
					j.TranscodePct = pct
				})
				broadcast(job)
			}
		}
	}()

	if err := cmd.Wait(); err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", fmt.Errorf("ffmpeg: %w", err)
	}

	job.Update(func(j *models.Job) {
		j.TranscodePct = 100
	})
	broadcast(job)

	return outputPath, nil
}
