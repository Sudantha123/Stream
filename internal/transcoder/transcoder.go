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

// ConvertToFMP4 converts input video to Fragmented MP4 (fMP4).
// Uses frag_keyframe+empty_moov+default_base_is_moof which writes fragments
// incrementally — global_sidx ඉවත් කළා, ඒ flag නිසා EOF වෙලාවේ
// සම්පූර්ණ file remux කරනවා (30-45s freeze).
// Progress stdout හරහා parse කරනවා (-progress pipe:1).
func ConvertToFMP4(ctx context.Context, job *models.Job, inputPath, outputDir string, broadcast func(*models.Job)) (string, error) {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return "", fmt.Errorf("mkdir: %w", err)
	}

	origName := filepath.Base(inputPath)
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
		"-loglevel", "error",
		"-progress", "pipe:1", // stdout හරහා progress
		"-i", inputPath,
		// global_sidx remove: EOF remux freeze නෑ
		// empty_moov+default_base_is_moof: streaming-safe fMP4
		"-movflags", "frag_keyframe+empty_moov+default_base_is_moof",
		"-codec", "copy",
		"-y",
		outputPath,
	}

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return "", fmt.Errorf("stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("ffmpeg start: %w", err)
	}

	// Progress stdout (pipe:1) scanner goroutine
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			line := scanner.Text()
			var outTimeSec float64
			if strings.HasPrefix(line, "out_time_ms=") {
				val, err := strconv.ParseFloat(strings.TrimPrefix(line, "out_time_ms="), 64)
				if err == nil && val > 0 {
					outTimeSec = val / 1e6
				}
			} else if m := timeRe.FindStringSubmatch(line); m != nil {
				h, _ := strconv.ParseFloat(m[1], 64)
				min, _ := strconv.ParseFloat(m[2], 64)
				sec, _ := strconv.ParseFloat(m[3], 64)
				outTimeSec = h*3600 + min*60 + sec
			}

			if outTimeSec > 0 && duration > 0 {
				pct := (outTimeSec / duration) * 100
				if pct > 99 {
					pct = 99
				}
				job.Update(func(j *models.Job) {
					j.TranscodePct = pct
				})
				broadcast(job)
			}
		}
	}()

	if err := cmd.Wait(); err != nil {
		<-progressDone
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", fmt.Errorf("ffmpeg: %w", err)
	}

	<-progressDone

	job.Update(func(j *models.Job) {
		j.TranscodePct = 100
	})
	broadcast(job)

	return outputPath, nil
}

