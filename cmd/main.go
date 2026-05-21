package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"streamvault/internal/server"
)

func main() {
	addr := flag.String("addr", ":5000", "listen address")
	dataDir := flag.String("data", "./data", "data directory")
	flag.Parse()

	// Ensure data directories
	for _, d := range []string{
		*dataDir,
		filepath.Join(*dataDir, "hls"),
		filepath.Join(*dataDir, "thumbs"),
		filepath.Join(*dataDir, "tmp"),
	} {
		if err := os.MkdirAll(d, 0755); err != nil {
			log.Fatalf("mkdir %s: %v", d, err)
		}
	}

	srv := server.New(*dataDir)

	log.Printf("StreamVault listening on %s", *addr)
	log.Printf("Gallery:   http://localhost%s/", *addr)
	log.Printf("Dashboard: http://localhost%s/admin", *addr)

	if err := http.ListenAndServe(*addr, srv); err != nil {
		log.Fatal(err)
	}
}
