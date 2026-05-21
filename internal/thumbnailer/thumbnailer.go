package thumbnailer

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

func Extract(inputPath, outputPath string, durationSec float64) error {
	seekTime := durationSec * 0.1
	if seekTime < 1 {
		seekTime = 1
	}

	args := []string{
		"-hide_banner",
		"-loglevel", "error",
		"-ss", formatTime(seekTime),
		"-i", inputPath,
		"-vframes", "1",
		"-vf", "scale=480:-2",
		"-q:v", "3",
		"-y",
		outputPath,
	}

	out, err := exec.Command("ffmpeg", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("ffmpeg thumbnail: %w\n%s", err, string(out))
	}
	return nil
}

func formatTime(sec float64) string {
	h := int(sec) / 3600
	m := (int(sec) % 3600) / 60
	s := sec - float64(h*3600+m*60)
	return fmt.Sprintf("%02d:%02d:%s", h, m, formatSeconds(s))
}

func formatSeconds(s float64) string {
	str := strconv.FormatFloat(s, 'f', 2, 64)
	if s < 10 {
		str = "0" + str
	}
	return strings.TrimRight(strings.TrimRight(str, "0"), ".")
}

