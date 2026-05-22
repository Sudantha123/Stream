package transcoder

import (
	"bufio"
	"context"
	"fmt"
	"math"
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

func Transcode(ctx context.Context, job *models.Job, inputPath, outputDir string, broadcast func(*models.Job)) error {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}

	duration, err := GetDuration(inputPath)
	if err != nil {
		duration = 0
	}

	segLen := job.SegmentLength
	if segLen <= 0 {
		segLen = 6
	}

	totalSegs := 0
	if duration > 0 {
		totalSegs = int(math.Ceil(duration / float64(segLen)))
	}

	job.Update(func(j *models.Job) {
		j.Status = models.StatusTranscoding
		j.TranscodeSegments = totalSegs
		j.TranscodeDone = 0
		j.TranscodePct = 0
	})
	broadcast(job)

	m3u8Path := filepath.Join(outputDir, "index.m3u8")
	segPattern := filepath.Join(outputDir, "seg%05d.m4s")
	initSeg := filepath.Join(outputDir, "init.mp4")

	// Ultra-fast: copy mode only, no re-encoding
	// fMP4 segments: better seek, lower storage, modern browser support
	args := []string{
		"-hide_banner",
		"-loglevel", "info",
		"-progress", "pipe:2",
		"-i", inputPath,
		"-c", "copy",           // NO re-encode
		"-avoid_negative_ts", "make_zero",
		"-f", "hls",
		"-hls_time", fmt.Sprintf("%d", segLen),
		"-hls_list_size", "0",
		"-hls_segment_type", "fmp4",
		"-hls_fmp4_init_filename", filepath.Base(initSeg),
		"-hls_segment_filename", segPattern,
		"-hls_flags", "independent_segments",
		m3u8Path,
	}

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("ffmpeg start: %w", err)
	}

	// Parse progress from ffmpeg -progress pipe:2
	go func() {
		scanner := bufio.NewScanner(stderr)
		var outTimeMs float64
		for scanner.Scan() {
			line := scanner.Text()
			// Parse out_time_ms=XXXXX
			if strings.HasPrefix(line, "out_time_ms=") {
				val, err := strconv.ParseFloat(strings.TrimPrefix(line, "out_time_ms="), 64)
				if err == nil {
					outTimeMs = val / 1e6 // to seconds
				}
			}
			// Also try time= from normal log
			if m := timeRe.FindStringSubmatch(line); m != nil {
				h, _ := strconv.ParseFloat(m[1], 64)
				min, _ := strconv.ParseFloat(m[2], 64)
				sec, _ := strconv.ParseFloat(m[3], 64)
				outTimeMs = h*3600 + min*60 + sec
			}

			if outTimeMs > 0 && duration > 0 {
				done := int(math.Floor(outTimeMs / float64(segLen)))
				pct := (outTimeMs / duration) * 100
				job.Update(func(j *models.Job) {
					j.TranscodeDone = done
					j.TranscodePct = pct
				})
				broadcast(job)
			}
		}
	}()

	if err := cmd.Wait(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("ffmpeg: %w", err)
	}

	// Final count of segments
	entries, _ := filepath.Glob(filepath.Join(outputDir, "*.m4s"))
	job.Update(func(j *models.Job) {
		j.TranscodeDone = len(entries)
		j.TranscodeSegments = len(entries)
		j.TranscodePct = 100
	})
	broadcast(job)

	return nil
}

