package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"

	"net/url"
	"path"

	"streamvault/internal/downloader"
	"streamvault/internal/models"
	"streamvault/internal/store"
	"streamvault/internal/thumbnailer"
	"streamvault/internal/transcoder"
)

type Server struct {
	store   *store.Store
	dataDir string
	router  *mux.Router

	wsMu      sync.Mutex
	wsClients map[*websocket.Conn]bool
	upgrader  websocket.Upgrader
}

func New(dataDir string) *Server {
	s := &Server{
		store:     store.New(dataDir),
		dataDir:   dataDir,
		wsClients: make(map[*websocket.Conn]bool),
		upgrader: websocket.Upgrader{
			CheckOrigin:     func(r *http.Request) bool { return true },
			ReadBufferSize:  1024,
			WriteBufferSize: 4096,
		},
	}
	s.router = s.buildRouter()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}

func (s *Server) buildRouter() *mux.Router {
	r := mux.NewRouter()

	// Static assets
	r.PathPrefix("/static/").Handler(http.StripPrefix("/static/",
		http.FileServer(http.Dir(filepath.Join(s.dataDir, "..", "web", "static")))))

	// fMP4 video files - serve with proper headers + range support
	r.PathPrefix("/videos/").HandlerFunc(s.serveVideo)

	// Thumbnails
	r.PathPrefix("/thumbs/").Handler(http.StripPrefix("/thumbs/",
		withCacheHeaders(http.FileServer(http.Dir(filepath.Join(s.dataDir, "thumbs"))), 86400)))

	// WebSocket
	r.HandleFunc("/ws", s.handleWS)

	// API
	api := r.PathPrefix("/api").Subrouter()
	api.HandleFunc("/jobs", s.listJobs).Methods("GET")
	api.HandleFunc("/jobs", s.createJob).Methods("POST")
	api.HandleFunc("/jobs/{id}/cancel", s.cancelJob).Methods("POST")
	api.HandleFunc("/videos", s.listVideos).Methods("GET")
	api.HandleFunc("/videos/search", s.searchVideos).Methods("GET")
	api.HandleFunc("/videos/{id}", s.getVideo).Methods("GET")
	api.HandleFunc("/videos/{id}", s.deleteVideo).Methods("DELETE")

	// Pages
	r.HandleFunc("/admin", s.adminPage)
	r.HandleFunc("/admin/", s.adminPage)
	r.HandleFunc("/watch/{id}", s.watchPage)
	r.HandleFunc("/", s.galleryPage)

	return r
}

// ── fMP4 video serving ───────────────────────────────────────────────────────

func (s *Server) serveVideo(w http.ResponseWriter, r *http.Request) {
	// Strip /videos/ prefix
	p := strings.TrimPrefix(r.URL.Path, "/videos/")
	full := filepath.Join(s.dataDir, "videos", filepath.Clean("/"+p))

	w.Header().Set("Content-Type", "video/mp4")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Accept-Ranges", "bytes")
	// Cache: fMP4 is immutable once written
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")

	http.ServeFile(w, r, full)
}

func withCacheHeaders(h http.Handler, maxAge int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d", maxAge))
		h.ServeHTTP(w, r)
	})
}

// ── WebSocket ────────────────────────────────────────────────────────────────

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	s.wsMu.Lock()
	s.wsClients[conn] = true
	s.wsMu.Unlock()

	jobs := s.store.AllJobs()
	for _, j := range jobs {
		s.sendToConn(conn, models.WSMessage{Type: "job_update", Data: j})
	}

	go func() {
		defer func() {
			s.wsMu.Lock()
			delete(s.wsClients, conn)
			s.wsMu.Unlock()
			conn.Close()
		}()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()
}

func (s *Server) broadcast(msg models.WSMessage) {
	data, _ := json.Marshal(msg)
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	for conn := range s.wsClients {
		conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
		conn.WriteMessage(websocket.TextMessage, data)
	}
}

func (s *Server) sendToConn(conn *websocket.Conn, msg models.WSMessage) {
	data, _ := json.Marshal(msg)
	conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	conn.WriteMessage(websocket.TextMessage, data)
}

func (s *Server) broadcastJob(job *models.Job) {
	s.broadcast(models.WSMessage{Type: "job_update", Data: job})
}

// ── API handlers ─────────────────────────────────────────────────────────────

func jsonOK(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func jsonErr(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func originalNameFromURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	base := path.Base(u.Path)
	if base == "" || base == "." || base == "/" {
		return ""
	}
	return base
}

func titleFromURL(rawURL string) string {
	base := originalNameFromURL(rawURL)
	if base == "" {
		return fmt.Sprintf("video-%s", time.Now().Format("20060102-150405"))
	}
	name := strings.TrimSuffix(base, path.Ext(base))
	name = strings.ReplaceAll(name, "_", " ")
	name = strings.ReplaceAll(name, "-", " ")
	if name == "" {
		return fmt.Sprintf("video-%s", time.Now().Format("20060102-150405"))
	}
	return name
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	jobs := s.store.AllJobs()
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].CreatedAt.After(jobs[j].CreatedAt)
	})
	jsonOK(w, jobs)
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	var req models.AddJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonErr(w, "invalid json", 400)
		return
	}
	if req.URL == "" {
		jsonErr(w, "url required", 400)
		return
	}

	origName := originalNameFromURL(req.URL)
	if origName == "" {
		origName = fmt.Sprintf("video-%s.mp4", time.Now().Format("20060102-150405"))
	}
	// Ensure .mp4 extension
	if !strings.HasSuffix(strings.ToLower(origName), ".mp4") {
		ext := filepath.Ext(origName)
		if ext != "" {
			origName = strings.TrimSuffix(origName, ext) + ".mp4"
		} else {
			origName = origName + ".mp4"
		}
	}

	if req.Title == "" {
		req.Title = titleFromURL(req.URL)
	}

	id := uuid.New().String()
	ctx, cancel := context.WithCancel(context.Background())

	job := &models.Job{
		ID:           id,
		URL:          req.URL,
		Title:        req.Title,
		OriginalName: origName,
		Status:       models.StatusPending,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
		CancelFunc:   cancel,
	}

	s.store.AddJob(job)
	s.broadcastJob(job)

	go s.processJob(ctx, job)

	jsonOK(w, job)
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	job, ok := s.store.GetJob(id)
	if !ok {
		jsonErr(w, "not found", 404)
		return
	}
	if job.CancelFunc != nil {
		job.CancelFunc()
	}
	job.Update(func(j *models.Job) {
		j.Status = models.StatusCancelled
	})
	s.broadcastJob(job)
	jsonOK(w, job)
}

func (s *Server) listVideos(w http.ResponseWriter, r *http.Request) {
	videos := s.store.AllVideos()
	sort.Slice(videos, func(i, j int) bool {
		return videos[i].CreatedAt.After(videos[j].CreatedAt)
	})
	jsonOK(w, videos)
}

func (s *Server) searchVideos(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	if q == "" {
		s.listVideos(w, r)
		return
	}
	results := s.store.SearchVideos(q)
	jsonOK(w, results)
}

func (s *Server) getVideo(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	v, ok := s.store.GetVideo(id)
	if !ok {
		jsonErr(w, "not found", 404)
		return
	}
	jsonOK(w, v)
}

func (s *Server) deleteVideo(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if err := s.store.DeleteVideo(id); err != nil {
		jsonErr(w, err.Error(), 404)
		return
	}
	os.Remove(filepath.Join(s.dataDir, "thumbs", id+".jpg"))
	s.broadcast(models.WSMessage{Type: "video_deleted", Data: id})
	jsonOK(w, map[string]bool{"ok": true})
}

// ── Job processor ────────────────────────────────────────────────────────────

func (s *Server) processJob(ctx context.Context, job *models.Job) {
	defer func() {
		tmpFile := filepath.Join(s.dataDir, "tmp", job.ID+".tmp")
		os.Remove(tmpFile)
	}()

	tmpDir := filepath.Join(s.dataDir, "tmp")
	os.MkdirAll(tmpDir, 0755)
	os.MkdirAll(filepath.Join(s.dataDir, "videos"), 0755)
	os.MkdirAll(filepath.Join(s.dataDir, "thumbs"), 0755)

	tmpFile := filepath.Join(tmpDir, job.ID+".tmp")

	// 1. Download
	log.Printf("[job %s] downloading %s", job.ID, job.URL)
	if err := downloader.Download(ctx, job, tmpFile, s.broadcastJob); err != nil {
		s.failJob(job, fmt.Sprintf("download: %v", err))
		return
	}

	if ctx.Err() != nil {
		return
	}

	// 2. Get duration
	duration, _ := transcoder.GetDuration(tmpFile)

	// 3. Thumbnail
	job.Update(func(j *models.Job) { j.Status = models.StatusThumbnailing })
	s.broadcastJob(job)

	thumbPath := filepath.Join(s.dataDir, "thumbs", job.ID+".jpg")
	thumbnailer.Extract(tmpFile, thumbPath, duration)

	// 4. Convert to fMP4 (fragmented, with global SIDX index)
	videosDir := filepath.Join(s.dataDir, "videos")
	log.Printf("[job %s] converting to fMP4: %s", job.ID, job.OriginalName)
	outputPath, err := transcoder.ConvertToFMP4(ctx, job, tmpFile, videosDir, s.broadcastJob)
	if err != nil {
		if ctx.Err() != nil {
			job.Update(func(j *models.Job) { j.Status = models.StatusCancelled })
			s.broadcastJob(job)
			return
		}
		s.failJob(job, fmt.Sprintf("convert: %v", err))
		return
	}

	// 5. Register video
	// FMP4Path = /videos/<original_name>
	fmp4Path := "/videos/" + job.OriginalName

	fi, _ := os.Stat(outputPath)
	var sizeBytes int64
	if fi != nil {
		sizeBytes = fi.Size()
	}

	video := &models.Video{
		ID:           job.ID,
		Title:        job.Title,
		OriginalName: job.OriginalName,
		Thumbnail:    "/thumbs/" + job.ID + ".jpg",
		Duration:     duration,
		SizeBytes:    sizeBytes,
		CreatedAt:    time.Now(),
		FMP4Path:     fmp4Path,
	}

	s.store.AddVideo(video)
	s.store.RemoveJob(job.ID)

	job.Update(func(j *models.Job) { j.Status = models.StatusDone })
	s.broadcast(models.WSMessage{Type: "job_done", Data: map[string]interface{}{
		"job":   job,
		"video": video,
	}})

	log.Printf("[job %s] done → %s (%s)", job.ID, video.Title, fmp4Path)
}

func (s *Server) failJob(job *models.Job, msg string) {
	log.Printf("[job %s] FAILED: %s", job.ID, msg)
	job.Update(func(j *models.Job) {
		j.Status = models.StatusFailed
		j.Error = msg
	})
	s.broadcastJob(job)
}

// ── Page handlers ────────────────────────────────────────────────────────────

func (s *Server) adminPage(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, filepath.Join(s.dataDir, "..", "web", "templates", "admin.html"))
}

func (s *Server) galleryPage(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, filepath.Join(s.dataDir, "..", "web", "templates", "gallery.html"))
}

func (s *Server) watchPage(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, filepath.Join(s.dataDir, "..", "web", "templates", "watch.html"))
}

