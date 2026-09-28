//go:build integration

package webhook_test

import (
	"bytes"
	"encoding/json"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bilirec/bilirec/internal/modules/bilibili"
	"github.com/bilirec/bilirec/internal/testutil/recording"
)

// BililiveRecorder Webhook v2 JSON (integration assertions).
type integrationWebhookEnvelope struct {
	EventType      string          `json:"EventType"`
	EventTimestamp string          `json:"EventTimestamp"`
	EventID        string          `json:"EventId"`
	EventData      json.RawMessage `json:"EventData"`
}

type integrationWebhookEventData struct {
	SessionID        string  `json:"SessionId"`
	RoomID           int     `json:"RoomId"`
	ShortID          int64   `json:"ShortId"`
	Name             string  `json:"Name"`
	Title            string  `json:"Title"`
	AreaNameParent   string  `json:"AreaNameParent"`
	AreaNameChild    string  `json:"AreaNameChild"`
	Recording        bool    `json:"Recording"`
	Streaming        bool    `json:"Streaming"`
	DanmakuConnected bool    `json:"DanmakuConnected"`
	RelativePath     string  `json:"RelativePath"`
	FileOpenTime     string  `json:"FileOpenTime"`
	FileCloseTime    string  `json:"FileCloseTime"`
	FileSize         int64   `json:"FileSize"`
	Duration         float64 `json:"Duration"`
}

type webhookIntegrationCollector struct {
	mu    sync.Mutex
	posts []integrationWebhookEnvelope
}

func (c *webhookIntegrationCollector) snapshot() []integrationWebhookEnvelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]integrationWebhookEnvelope, len(c.posts))
	copy(out, c.posts)
	return out
}

func startWebhookIntegrationCollector(t *testing.T) (baseURL string, collector *webhookIntegrationCollector) {
	t.Helper()

	collector = &webhookIntegrationCollector{}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen webhook collector: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var env integrationWebhookEnvelope
		if err := json.Unmarshal(body, &env); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		collector.mu.Lock()
		collector.posts = append(collector.posts, env)
		collector.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Handler: mux}
	go func() {
		if serveErr := srv.Serve(ln); serveErr != nil && serveErr != http.ErrServerClosed {
			t.Logf("webhook collector stopped: %v", serveErr)
		}
	}()
	t.Cleanup(func() { _ = srv.Close() })

	return "http://" + ln.Addr().String(), collector
}

const (
	webhookRequiredSessionStarted = "SessionStarted"
	webhookRequiredFileOpening    = "FileOpening"
	webhookRequiredFileClosed     = "FileClosed"
	webhookRequiredSessionEnded   = "SessionEnded"

	// recorder.finalize skips FileClosed (and may delete the file) under 1KB.
	webhookKeptSegmentMinBytes int64 = 1024
)

func waitForWebhookLifecycle(
	t *testing.T,
	collector *webhookIntegrationCollector,
	roomID int,
	outputDir string,
	timeout time.Duration,
) []integrationWebhookEnvelope {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		posts := filterWebhookPostsForRoom(collector.snapshot(), roomID)
		if webhookLifecycleSettled(posts, outputDir) {
			return posts
		}
		if time.Now().After(deadline) {
			got := eventTypeCounts(posts)
			t.Fatalf("webhook drain timeout for room %d; got %v", roomID, got)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func webhookLifecycleSettled(posts []integrationWebhookEnvelope, outputDir string) bool {
	counts := eventTypeCounts(posts)
	if counts[webhookRequiredSessionStarted] < 1 ||
		counts[webhookRequiredSessionEnded] < 1 ||
		counts[webhookRequiredFileOpening] < 1 ||
		counts[webhookRequiredFileClosed] < 1 {
		return false
	}
	closed := webhookRelativePaths(posts, webhookRequiredFileClosed)
	for rel := range webhookRelativePaths(posts, webhookRequiredFileOpening) {
		if closed[rel] {
			continue
		}
		abs := filepath.Join(outputDir, filepath.FromSlash(rel))
		stat, err := os.Stat(abs)
		if err == nil && stat.Size() >= webhookKeptSegmentMinBytes {
			return false
		}
	}
	return true
}

func webhookRelativePaths(posts []integrationWebhookEnvelope, eventType string) map[string]bool {
	out := make(map[string]bool)
	for _, p := range posts {
		if p.EventType != eventType {
			continue
		}
		data, ok := parseIntegrationWebhookData(p.EventData)
		if !ok || data.RelativePath == "" {
			continue
		}
		out[data.RelativePath] = true
	}
	return out
}

func filterWebhookPostsForRoom(posts []integrationWebhookEnvelope, roomID int) []integrationWebhookEnvelope {
	var out []integrationWebhookEnvelope
	for _, p := range posts {
		data, ok := parseIntegrationWebhookData(p.EventData)
		if !ok || data.RoomID != roomID {
			continue
		}
		out = append(out, p)
	}
	return out
}

func parseIntegrationWebhookData(raw json.RawMessage) (integrationWebhookEventData, bool) {
	if len(raw) == 0 {
		return integrationWebhookEventData{}, false
	}
	var data integrationWebhookEventData
	if err := json.Unmarshal(raw, &data); err != nil {
		return integrationWebhookEventData{}, false
	}
	return data, true
}

func eventTypeCounts(posts []integrationWebhookEnvelope) map[string]int {
	counts := make(map[string]int)
	for _, p := range posts {
		counts[p.EventType]++
	}
	return counts
}

func assertWebhookRecordingLifecycle(
	t *testing.T,
	posts []integrationWebhookEnvelope,
	roomID int,
	outputDir string,
	outputPath string,
	room *bilibili.LiveRoomInfoDetail,
) {
	t.Helper()

	byType := make(map[string][]integrationWebhookEnvelope)
	for _, p := range posts {
		byType[p.EventType] = append(byType[p.EventType], p)
	}

	sessionStarted := byType[webhookRequiredSessionStarted]
	if len(sessionStarted) != 1 {
		t.Fatalf("SessionStarted count %d, want 1", len(sessionStarted))
	}
	sessionEnded := byType[webhookRequiredSessionEnded]
	if len(sessionEnded) != 1 {
		t.Fatalf("SessionEnded count %d, want 1", len(sessionEnded))
	}
	fileOpening := byType[webhookRequiredFileOpening]
	if len(fileOpening) < 1 {
		t.Fatal("missing FileOpening")
	}
	fileClosed := byType[webhookRequiredFileClosed]
	if len(fileClosed) < 1 {
		t.Fatal("missing FileClosed")
	}

	sessionData, ok := parseIntegrationWebhookData(sessionStarted[0].EventData)
	if !ok || sessionData.SessionID == "" {
		t.Fatal("SessionStarted missing SessionId")
	}
	if sessionData.RoomID != roomID {
		t.Fatalf("SessionStarted RoomId %d", sessionData.RoomID)
	}
	if !sessionData.Recording {
		t.Fatal("SessionStarted expected Recording=true")
	}

	openByPath := make(map[string]integrationWebhookEventData, len(fileOpening))
	for i, p := range fileOpening {
		data, ok := parseIntegrationWebhookData(p.EventData)
		if !ok || data.SessionID != sessionData.SessionID {
			t.Fatalf("FileOpening[%d] SessionId mismatch: %q", i, data.SessionID)
		}
		if data.RelativePath == "" {
			t.Fatalf("FileOpening[%d] missing RelativePath", i)
		}
		if _, dup := openByPath[data.RelativePath]; dup {
			t.Fatalf("duplicate FileOpening for %q", data.RelativePath)
		}
		openByPath[data.RelativePath] = data
	}

	closedPaths := make(map[string]struct{}, len(fileClosed))
	for i, p := range fileClosed {
		data, ok := parseIntegrationWebhookData(p.EventData)
		if !ok || data.SessionID != sessionData.SessionID {
			t.Fatalf("FileClosed[%d] SessionId mismatch: %q", i, data.SessionID)
		}
		if _, ok := openByPath[data.RelativePath]; !ok {
			t.Fatalf("FileClosed RelativePath %q has no FileOpening", data.RelativePath)
		}
		if data.FileCloseTime == "" || data.FileOpenTime == "" {
			t.Fatalf("FileClosed[%d] missing file times", i)
		}
		if data.FileSize <= 0 {
			t.Fatalf("FileClosed[%d] FileSize %d", i, data.FileSize)
		}
		if data.Duration < 0 {
			t.Fatalf("FileClosed[%d] Duration %v", i, data.Duration)
		}
		abs := filepath.Join(outputDir, filepath.FromSlash(data.RelativePath))
		stat, err := os.Stat(abs)
		if err != nil {
			t.Fatalf("stat FileClosed %s: %v", data.RelativePath, err)
		}
		if data.FileSize != stat.Size() {
			t.Fatalf("FileClosed FileSize %d != file %d path=%s", data.FileSize, stat.Size(), data.RelativePath)
		}
		closedPaths[data.RelativePath] = struct{}{}
	}

	for rel := range openByPath {
		if _, ok := closedPaths[rel]; ok {
			continue
		}
		abs := filepath.Join(outputDir, filepath.FromSlash(rel))
		stat, err := os.Stat(abs)
		if err == nil && stat.Size() >= webhookKeptSegmentMinBytes {
			t.Fatalf("FileOpening %q has no FileClosed; file still present (%d bytes)", rel, stat.Size())
		}
	}

	endData, ok := parseIntegrationWebhookData(sessionEnded[0].EventData)
	if !ok || endData.SessionID != sessionData.SessionID {
		t.Fatalf("SessionEnded SessionId mismatch: %q", endData.SessionID)
	}
	if endData.Recording {
		t.Fatal("SessionEnded expected Recording=false")
	}

	wantRel, err := filepath.Rel(outputDir, outputPath)
	if err != nil {
		t.Fatalf("rel output path: %v", err)
	}
	wantRel = filepath.ToSlash(wantRel)
	if _, ok := openByPath[wantRel]; !ok {
		t.Fatalf("captured output path %q missing FileOpening; got %v", wantRel, slices.Sorted(maps.Keys(openByPath)))
	}

	if room != nil {
		if sessionData.Name != room.Uname {
			t.Fatalf("Name %q want %q", sessionData.Name, room.Uname)
		}
		if sessionData.Title != room.Title {
			t.Fatalf("Title %q want %q", sessionData.Title, room.Title)
		}
		if int64(roomID) != room.RoomID {
			t.Fatalf("room id mismatch")
		}
		if sessionData.ShortID != room.ShortID {
			t.Fatalf("ShortId %d want %d", sessionData.ShortID, room.ShortID)
		}
	}

	for _, p := range posts {
		if p.EventID == "" {
			t.Fatalf("%s missing EventId", p.EventType)
		}
		if strings.TrimSpace(p.EventTimestamp) == "" {
			t.Fatalf("%s missing EventTimestamp", p.EventType)
		}
	}

	idx := func(et string) int {
		for i, p := range posts {
			if p.EventType == et {
				return i
			}
		}
		return -1
	}
	// SessionEnded is emitted from Stop(); FileClosed follows async finalize and may
	// trail later FileOpening events when a live session rotates mid-record.
	if idx(webhookRequiredSessionStarted) < 0 || idx(webhookRequiredFileOpening) < 0 ||
		idx(webhookRequiredFileClosed) < 0 || idx(webhookRequiredSessionEnded) < 0 {
		t.Fatal("missing required webhook events in delivery order")
	}
	if idx(webhookRequiredFileOpening) <= idx(webhookRequiredSessionStarted) {
		t.Fatalf("FileOpening at %d must follow SessionStarted at %d",
			idx(webhookRequiredFileOpening), idx(webhookRequiredSessionStarted))
	}
	if idx(webhookRequiredFileClosed) <= idx(webhookRequiredFileOpening) {
		t.Fatalf("FileClosed at %d must follow FileOpening at %d",
			idx(webhookRequiredFileClosed), idx(webhookRequiredFileOpening))
	}
	if idx(webhookRequiredSessionEnded) <= idx(webhookRequiredSessionStarted) {
		t.Fatalf("SessionEnded at %d must follow SessionStarted at %d",
			idx(webhookRequiredSessionEnded), idx(webhookRequiredSessionStarted))
	}
}

func runWebhookIntegrationRecordTest(t *testing.T) {
	t.Helper()

	outputDir := t.TempDir()
	webhookURL, collector := startWebhookIntegrationCollector(t)
	t.Setenv("OUTPUT_DIR", outputDir)
	t.Setenv("WEBHOOK_URLS", webhookURL)

	sess := recording.NewSession(t)
	roomID := recording.ResolveLiveTestRoomID(t, sess.Room)
	roomInfo, err := sess.Room.GetLiveRoomInfo(roomID)
	if err != nil {
		t.Fatalf("GetLiveRoomInfo: %v", err)
	}

	startErr := sess.Recorder.Start(roomID)
	recording.HandleRecordingStartErr(t, startErr)

	outputPath := recording.WaitForOutputPathAfterStart(t, sess.Recorder, roomID)
	recordDuration := recording.IntegrationRecordDuration()
	t.Logf("webhook integration: recording room=%d for %s (webhook=%s)", roomID, recordDuration, webhookURL)
	_ = sess.Monitor.RunRecordingProfiledWait(t, "webhook_recording", recordDuration)

	recording.StopRecording(t, sess.Recorder, roomID)
	recording.WaitUntilNoActiveRecordings(t, sess.Recorder, 30*time.Second)
	time.Sleep(recording.SettleAfterStop)

	drainTimeout := 3 * time.Minute
	if os.Getenv("CI") != "" {
		drainTimeout = 5 * time.Minute
	}
	posts := waitForWebhookLifecycle(t, collector, roomID, outputDir, drainTimeout)
	assertWebhookRecordingLifecycle(t, posts, roomID, outputDir, outputPath, roomInfo)

	t.Logf("webhook integration ok: %d events for room %d FileOpening=%d FileClosed=%d",
		len(posts), roomID,
		eventTypeCounts(posts)[webhookRequiredFileOpening],
		eventTypeCounts(posts)[webhookRequiredFileClosed])
}

func TestWebhookRecordingLifecycle_AllowsRotatedSegments(t *testing.T) {
	outputDir := t.TempDir()
	roomID := 1754796401
	sessionID := "sess-rotate"
	room := &bilibili.LiveRoomInfoDetail{
		RoomID:  int64(roomID),
		ShortID: 3,
		Uname:   "anchor",
		Title:   "title",
	}
	first := writeWebhookTestSegment(t, outputDir, "anchor-1754796401/title-20260927_144057.flv", 2048)
	second := writeWebhookTestSegment(t, outputDir, "anchor-1754796401/title-20260927_144057-1.flv", 4096)
	firstRel := webhookTestRel(t, outputDir, first)
	secondRel := webhookTestRel(t, outputDir, second)

	posts := []integrationWebhookEnvelope{
		mustWebhookPost(t, webhookRequiredSessionStarted, webhookTestData(room, sessionID, "", 0, true)),
		mustWebhookPost(t, webhookRequiredFileOpening, webhookTestData(room, sessionID, firstRel, 0, true)),
		mustWebhookPost(t, webhookRequiredFileOpening, webhookTestData(room, sessionID, secondRel, 0, true)),
		mustWebhookPost(t, webhookRequiredFileClosed, webhookTestClosedData(room, sessionID, firstRel, 2048)),
		mustWebhookPost(t, webhookRequiredSessionEnded, webhookTestData(room, sessionID, "", 0, false)),
		mustWebhookPost(t, webhookRequiredFileClosed, webhookTestClosedData(room, sessionID, secondRel, 4096)),
	}
	if !webhookLifecycleSettled(posts, outputDir) {
		t.Fatal("rotated session should be settled once every kept FileOpening has FileClosed")
	}
	assertWebhookRecordingLifecycle(t, posts, roomID, outputDir, first, room)
}

func TestWebhookLifecycleSettled_WaitsForKeptSegmentClose(t *testing.T) {
	outputDir := t.TempDir()
	roomID := 42
	sessionID := "sess-wait"
	room := &bilibili.LiveRoomInfoDetail{RoomID: int64(roomID), Uname: "a", Title: "t"}
	first := writeWebhookTestSegment(t, outputDir, "a-42/clip.flv", 2048)
	second := writeWebhookTestSegment(t, outputDir, "a-42/clip-1.flv", 4096)
	firstRel := webhookTestRel(t, outputDir, first)
	secondRel := webhookTestRel(t, outputDir, second)

	partial := []integrationWebhookEnvelope{
		mustWebhookPost(t, webhookRequiredSessionStarted, webhookTestData(room, sessionID, "", 0, true)),
		mustWebhookPost(t, webhookRequiredFileOpening, webhookTestData(room, sessionID, firstRel, 0, true)),
		mustWebhookPost(t, webhookRequiredFileOpening, webhookTestData(room, sessionID, secondRel, 0, true)),
		mustWebhookPost(t, webhookRequiredFileClosed, webhookTestClosedData(room, sessionID, firstRel, 2048)),
		mustWebhookPost(t, webhookRequiredSessionEnded, webhookTestData(room, sessionID, "", 0, false)),
	}
	if webhookLifecycleSettled(partial, outputDir) {
		t.Fatal("must wait for FileClosed of a kept rotated segment")
	}

	complete := append(slices.Clip(partial),
		mustWebhookPost(t, webhookRequiredFileClosed, webhookTestClosedData(room, sessionID, secondRel, 4096)))
	if !webhookLifecycleSettled(complete, outputDir) {
		t.Fatal("settled after matching FileClosed arrived")
	}
}

func writeWebhookTestSegment(t *testing.T, outputDir, rel string, size int) string {
	t.Helper()
	abs := filepath.Join(outputDir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(abs), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(abs, bytes.Repeat([]byte("x"), size), 0644); err != nil {
		t.Fatal(err)
	}
	return abs
}

func webhookTestRel(t *testing.T, outputDir, abs string) string {
	t.Helper()
	rel, err := filepath.Rel(outputDir, abs)
	if err != nil {
		t.Fatal(err)
	}
	return filepath.ToSlash(rel)
}

func webhookTestData(room *bilibili.LiveRoomInfoDetail, sessionID, rel string, size int64, recording bool) integrationWebhookEventData {
	return integrationWebhookEventData{
		SessionID:    sessionID,
		RoomID:       int(room.RoomID),
		ShortID:      room.ShortID,
		Name:         room.Uname,
		Title:        room.Title,
		Recording:    recording,
		RelativePath: rel,
		FileSize:     size,
	}
}

func webhookTestClosedData(room *bilibili.LiveRoomInfoDetail, sessionID, rel string, size int64) integrationWebhookEventData {
	data := webhookTestData(room, sessionID, rel, size, true)
	data.FileOpenTime = "2026-09-27T14:40:57Z"
	data.FileCloseTime = "2026-09-27T14:46:16Z"
	data.Duration = 319
	return data
}

func mustWebhookPost(t *testing.T, eventType string, data integrationWebhookEventData) integrationWebhookEnvelope {
	t.Helper()
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	return integrationWebhookEnvelope{
		EventType:      eventType,
		EventTimestamp: "2026-09-27T14:40:57.000000000Z",
		EventID:        eventType + "-" + data.RelativePath,
		EventData:      raw,
	}
}

// Long-running live recording with WEBHOOK_URLS pointed at a local collector.
// Isolated run (CI runs separately from default recorder integration):
//
//	go test ./internal/services/webhook -run TestLong_WebhookDuringRecord -count=1 -timeout 30m
func TestLong_WebhookDuringRecord(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping webhook integration record test in short mode")
	}
	runWebhookIntegrationRecordTest(t)
}
