// Command coordinator starts the MapReduce coordinator process, managing job phase transitions and assigning tasks to workers.
package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"mapreduce/internal/coordinator"
	"mapreduce/internal/core"
	"mapreduce/internal/storage"
	"mapreduce/internal/transport"
)

func main() {
	// 1. Parse command-line configuration flags.
	addr := flag.String("addr", ":9090", "gRPC listen address")
	inputFlag := flag.String("input", "", "input file pattern or list")
	nReduce := flag.Int("nreduce", 10, "number of reduce partitions")
	storageType := flag.String("storage", "disk", "storage type (disk or s3)")
	s3Endpoint := flag.String("s3-endpoint", "localhost:9000", "S3 endpoint")
	s3Bucket := flag.String("s3-bucket", "mapreduce", "S3 bucket name")
	s3AccessKey := flag.String("s3-access-key", "minioadmin", "S3 access key")
	s3SecretKey := flag.String("s3-secret-key", "minioadmin", "S3 secret key")
	metricsAddr := flag.String("metrics-addr", ":9091", "Prometheus metrics address")
	flag.Parse()

	// 2. Gather and resolve all input files (handling comma-separated lists and wildcard glob patterns).
	var files []string
	if *inputFlag != "" {
		if strings.Contains(*inputFlag, "*") {
			matches, err := filepath.Glob(*inputFlag)
			if err != nil {
				log.Fatalf("failed to glob input: %v", err)
			}
			files = append(files, matches...)
		} else {
			files = append(files, strings.Split(*inputFlag, ",")...)
		}
	}
	// Also add any remaining non-flag arguments
	for _, arg := range flag.Args() {
		if strings.Contains(arg, "*") {
			matches, err := filepath.Glob(arg)
			if err == nil {
				files = append(files, matches...)
			}
		} else {
			files = append(files, arg)
		}
	}

	if len(files) == 0 {
		log.Fatal("no input files specified")
	}

	log.Printf("[Coordinator] Starting job with %d input files, %d reduce partitions", len(files), *nReduce)

	// 3. Initialize the storage adapter layer (either local disk storage or S3-compatible object storage).
	var store core.Storage
	var err error
	if *storageType == "s3" {
		store, err = storage.NewS3Storage(*s3Endpoint, *s3Bucket, *s3AccessKey, *s3SecretKey)
		if err != nil {
			log.Fatalf("failed to initialize S3 storage: %v", err)
		}
	} else {
		store = storage.NewDiskStorage(".")
	}

	// 4. If using S3 storage, pre-upload the local input files to the target S3 bucket.
	if *storageType == "s3" {
		for _, f := range files {
			log.Printf("[Coordinator] Uploading input file %s to S3...", f)
			lf, err := os.Open(f)
			if err != nil {
				log.Fatalf("failed to open local input file %s: %v", f, err)
			}
			baseName := filepath.Base(f)
			err = store.Write(baseName, lf)
			lf.Close()
			if err != nil {
				log.Fatalf("failed to upload input file %s to S3: %v", f, err)
			}
		}
		// Map files slice to use S3 object names (basenames)
		for i, f := range files {
			files[i] = filepath.Base(f)
		}
	}

	// 5. Start a background HTTP server to serve Prometheus metrics for monitoring coordinator state.
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		log.Printf("[Coordinator] Serving metrics on %s/metrics", *metricsAddr)
		if err := http.ListenAndServe(*metricsAddr, mux); err != nil {
			log.Printf("[Coordinator] Metrics server failed: %v", err)
		}
	}()

	// 6. Instantiate the coordinator engine core and bind it to the gRPC transport server.
	coord := coordinator.New(files, *nReduce)
	srv := transport.NewGRPCServer(coord, *addr)

	log.Printf("[Coordinator] Serving gRPC on %s", *addr)
	if err := srv.Serve(); err != nil {
		log.Fatalf("gRPC server failed: %v", err)
	}
}
