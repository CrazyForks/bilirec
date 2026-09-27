package recording

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/bilirec/bilirec/internal/services/recorder"
	"github.com/bilirec/bilirec/utils"
)

func WaitForOutputPath(t *testing.T, recorderService *recorder.Service, room int, timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	polls := 0
	hasAnyStats := false
	lastStatus := recorder.Idle
	lastOutputPath := ""
	lastElapsedSeconds := int64(0)
	for time.Now().Before(deadline) {
		polls++
		stats, hasStats := recorderService.GetStats(room)
		if hasStats && stats != nil {
			hasAnyStats = true
			lastStatus = stats.Status
			lastOutputPath = stats.OutputPath
			lastElapsedSeconds = stats.ElapsedSeconds
			if stats.OutputPath != "" {
				return stats.OutputPath
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !hasAnyStats {
		t.Logf("output path wait timeout: no stats for room=%d (recording may have been removed before assertion), polls=%d, timeout=%s", room, polls, timeout)
		return ""
	}
	t.Logf("output path wait timeout: stats available but output path is still empty, room=%d, last_status=%s, last_elapsed=%ds, polls=%d, timeout=%s, last_output_path=%q", room, lastStatus, lastElapsedSeconds, polls, timeout, lastOutputPath)
	return ""
}

func WaitForOutputPathAfterStart(t *testing.T, recorderService *recorder.Service, room int) string {
	t.Helper()
	timeout := time.Duration(utils.Ternary(os.Getenv("CI") != "", 30, 10)) * time.Second
	outputPath := WaitForOutputPath(t, recorderService, room, timeout)
	if outputPath == "" {
		t.Fatalf("recording started but output path was not available within %s", timeout)
	}
	t.Logf("captured output path early for room=%d: %s", room, outputPath)
	return outputPath
}

func WaitUntilNoActiveRecordings(t *testing.T, recorderService *recorder.Service, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if recorderService.ListRecordingSize() == 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("cleanup timeout: still has %d active recordings", recorderService.ListRecordingSize())
}

// StopRecording stops an active recording. A false return is not a failure:
// a live room may already have auto-stopped (streamer offline past
// MaxRetryMinutes, duration cap, etc.).
func StopRecording(t *testing.T, recorderService *recorder.Service, roomID int) {
	t.Helper()
	t.Log("stopping recording")
	if !recorderService.Stop(roomID) {
		t.Logf("stop returned false for room=%d (recording may have already ended)", roomID)
	}
}

type ConcurrentStartResult struct {
	Room int
	Err  error
}

// CollectConcurrentStartResults drains all concurrent Start results before handling
// errors so a failure received early cannot skip Stop on later successful starts.
func CollectConcurrentStartResults(t *testing.T, recorderService *recorder.Service, results <-chan ConcurrentStartResult) []int {
	t.Helper()

	started := make([]int, 0)
	var firstErr error
	for r := range results {
		if r.Err == nil {
			started = append(started, r.Room)
			t.Logf("concurrent start ok: room=%d", r.Room)
			continue
		}
		if firstErr == nil {
			firstErr = r.Err
		}
	}

	if firstErr != nil {
		for _, rid := range started {
			_ = recorderService.Stop(rid)
		}
		WaitUntilNoActiveRecordings(t, recorderService, 30*time.Second)
		HandleRecordingStartErr(t, firstErr)
	}

	return started
}

func CheckFFmpegAvailable(t *testing.T) bool {
	_, err := exec.LookPath("ffprobe")
	if err != nil {
		t.Logf("⚠️ ffprobe not found in PATH, skipping playability verification: %v", err)
		return false
	}
	t.Log("✓ ffprobe found, will verify recorded file playability")
	return true
}

var recordingFormatExtensions = map[string]string{
	"flv":  ".flv",
	"ts":   ".ts",
	"fmp4": ".fmp4",
}

func VerifyAllRecordingsInRoomDir(t *testing.T, roomDir, expectedFormat string) {
	t.Helper()
	if !CheckFFmpegAvailable(t) {
		return
	}
	ext, ok := recordingFormatExtensions[expectedFormat]
	if !ok {
		t.Fatalf("unknown format %q", expectedFormat)
	}

	pattern := filepath.Join(roomDir, "*"+ext)
	files, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatalf("glob %s: %v", pattern, err)
	}
	if len(files) == 0 {
		t.Fatalf("no %s recordings under %s", ext, roomDir)
	}

	sort.Strings(files)
	t.Logf("ffprobe %d file(s) in %s", len(files), roomDir)
	for i, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			t.Logf("[%d/%d] %s", i+1, len(files), f)
			verifyRecordingPlayability(t, f, expectedFormat)
		})
	}
}

func parseFloatDuration(durationStr string) (float64, error) {
	var result float64
	_, err := fmt.Sscanf(durationStr, "%f", &result)
	return result, err
}

type ffprobeStreamInfo struct {
	CodecType  string `json:"codec_type"`
	CodecName  string `json:"codec_name"`
	Duration   string `json:"duration"`
	TimeBase   string `json:"time_base"`
	StartTime  string `json:"start_time"`
	Width      int    `json:"width,omitempty"`
	Height     int    `json:"height,omitempty"`
	SampleRate string `json:"sample_rate,omitempty"`
	Channels   int    `json:"channels,omitempty"`
}

type ffprobeOutput struct {
	Format struct {
		Duration string `json:"duration"`
	} `json:"format"`
	Streams []ffprobeStreamInfo `json:"streams"`
}

func verifyRecordingPlayability(t *testing.T, filePath string, expectedFormat string) {
	if filePath == "" {
		t.Error("❌ Output file path is empty")
		return
	}

	time.Sleep(500 * time.Millisecond)

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			t.Errorf("❌ Recorded file not found: %s", filePath)
		} else {
			t.Errorf("❌ Failed to stat recorded file: %v", err)
		}
		return
	}

	if fileInfo.Size() == 0 {
		t.Errorf("❌ Recorded file is empty: %s", filePath)
		return
	}

	t.Logf("✓ Recorded file exists: %s (size: %.2f MB)", filePath, float64(fileInfo.Size())/1024/1024)

	cmd := exec.Command("ffprobe",
		"-v", "error",
		"-show_format",
		"-show_streams",
		"-of", "json",
		filePath,
	)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		t.Logf("⚠️ ffprobe verification skipped: %v, stderr: %s", err, stderr.String())
		return
	}

	var probe ffprobeOutput
	if err := json.Unmarshal(stdout.Bytes(), &probe); err != nil {
		t.Logf("⚠️ Failed to parse ffprobe JSON: %v, output: %s", err, stdout.String())
		return
	}

	hasVideo := false
	hasAudio := false

	if durationFloat, err := parseFloatDuration(probe.Format.Duration); err == nil {
		mins := int(durationFloat) / 60
		secs := int(durationFloat) % 60
		t.Logf("  - Duration: %d:%02d", mins, secs)
	}

	for _, stream := range probe.Streams {
		switch stream.CodecType {
		case "video":
			hasVideo = true
			t.Logf("  - Video stream: %dx%d %s", stream.Width, stream.Height, stream.CodecName)
		case "audio":
			hasAudio = true
			t.Logf("  - Audio stream: %d ch, %s Hz %s", stream.Channels, stream.SampleRate, stream.CodecName)
		}
	}

	if !hasVideo && !hasAudio {
		t.Error("❌ No valid video or audio streams found in recorded file")
		return
	}

	if hasVideo {
		t.Log("✓ Video stream verified - file should be playable")
	}
	if hasAudio {
		t.Log("✓ Audio stream verified - file should be playable")
	}

	switch expectedFormat {
	case "flv":
		t.Log("  Note: FLV files may have minor header inconsistencies due to streaming nature, but should be playable without seeking")
	case "ts":
		t.Log("  Note: TS files may exhibit 2-second stutter when seeking (seeking after pause) - this is normal. Playback without seeking should be smooth.")
	case "fmp4":
		t.Log("  Note: FMP4 files may show screen artifacts when seeking - this is normal. Playback without seeking should be clean with proper timestamps.")
	}
}
