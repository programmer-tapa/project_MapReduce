// Command worker starts a MapReduce worker process, which queries the coordinator for tasks and executes them.
package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"mapreduce/internal/core"
	"mapreduce/internal/storage"
	"mapreduce/internal/worker"
)

func main() {
	// 1. Parse and validate command-line configuration flags.
	coordinatorAddr := flag.String("coordinator-addr", "localhost:9090", "gRPC address of the coordinator")
	appCommand := flag.String("app", "", "Map/Reduce application executable script or binary")
	storageType := flag.String("storage", "disk", "storage type (disk or s3)")
	s3Endpoint := flag.String("s3-endpoint", "localhost:9000", "S3 endpoint")
	s3Bucket := flag.String("s3-bucket", "mapreduce", "S3 bucket name")
	s3AccessKey := flag.String("s3-access-key", "minioadmin", "S3 access key")
	s3SecretKey := flag.String("s3-secret-key", "minioadmin", "S3 secret key")
	metricsAddr := flag.String("metrics-addr", ":9092", "Prometheus metrics address")
	flag.Parse()

	if *appCommand == "" {
		log.Fatal("no Map/Reduce application specified (--app)")
	}

	log.Printf("[Worker] Initializing storage adapter: %s...", *storageType)

	// 2. Initialize the storage adapter layer (either local disk storage or S3-compatible object storage).
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

	// 3. Start a background HTTP server to serve Prometheus metrics for monitoring worker tasks execution.
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		log.Printf("[Worker] Serving metrics on %s/metrics", *metricsAddr)
		if err := http.ListenAndServe(*metricsAddr, mux); err != nil {
			log.Printf("[Worker] Metrics server failed: %v", err)
		}
	}()

	// 4. Instantiate the worker and start the polling loop to request and run tasks.
	w := worker.New(*coordinatorAddr, *appCommand, store)
	if err := w.Run(); err != nil {
		log.Fatalf("Worker execution failed: %v", err)
	}
}
