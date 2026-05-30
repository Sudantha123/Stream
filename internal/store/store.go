package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"streamvault/internal/models"
)

type Store struct {
	mu      sync.RWMutex
	jobs    map[string]*models.Job
	videos  map[string]*models.Video
	dataDir string
}

func New(dataDir string) *Store {
	s := &Store{
		jobs:    make(map[string]*models.Job),
		videos:  make(map[string]*models.Video),
		dataDir: dataDir,
	}
	s.loadVideos()
	return s
}

// Jobs

func (s *Store) AddJob(job *models.Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[job.ID] = job
}

func (s *Store) GetJob(id string) (*models.Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[id]
	return j, ok
}

func (s *Store) AllJobs() []*models.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*models.Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		out = append(out, j)
	}
	return out
}

func (s *Store) RemoveJob(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.jobs, id)
}

// Videos

func (s *Store) AddVideo(v *models.Video) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.videos[v.ID] = v
	s.saveVideos()
}

func (s *Store) GetVideo(id string) (*models.Video, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.videos[id]
	return v, ok
}

func (s *Store) AllVideos() []*models.Video {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*models.Video, 0, len(s.videos))
	for _, v := range s.videos {
		out = append(out, v)
	}
	return out
}

func (s *Store) SearchVideos(query string) []*models.Video {
	s.mu.RLock()
	defer s.mu.RUnlock()
	q := strings.ToLower(query)
	out := make([]*models.Video, 0)
	for _, v := range s.videos {
		if strings.Contains(strings.ToLower(v.Title), q) {
			out = append(out, v)
		}
	}
	return out
}

func (s *Store) DeleteVideo(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	v, ok := s.videos[id]
	if !ok {
		return fmt.Errorf("video not found")
	}
	// Remove the fMP4 video file from /data/videos/
	videoFile := filepath.Join(s.dataDir, "videos", v.OriginalName)
	os.Remove(videoFile)
	delete(s.videos, id)
	s.saveVideos()
	return nil
}

func (s *Store) saveVideos() {
	path := filepath.Join(s.dataDir, "videos.json")
	data, _ := json.MarshalIndent(s.videos, "", "  ")
	os.WriteFile(path, data, 0644)
}

func (s *Store) loadVideos() {
	path := filepath.Join(s.dataDir, "videos.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	json.Unmarshal(data, &s.videos)
}


