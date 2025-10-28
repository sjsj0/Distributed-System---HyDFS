package hydfs_daemon

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"context"
	"hydfs-g33/config"
	"hydfs-g33/hydfs/cmd/server"
	"hydfs-g33/hydfs/logging"
	"hydfs-g33/hydfs/ring"
	"hydfs-g33/hydfs/routing"
	"hydfs-g33/hydfs/storage"
	nodeid "hydfs-g33/membership/node"
	"hydfs-g33/membership/store"
)

type HyDFSDaemon struct {
	fsPath    *storage.FSPaths
	fileStore *storage.FileStore
	log       *logging.Logger
	cfg       config.Config

	httpSrv *http.Server
	cancel  context.CancelFunc

	ringMgr *ring.Manager
	router  *routing.Router
}

// Run starts the HTTP node, sets up ring manager (subscribes to membership updates),
// and constructs a router (RF=3 by default).
func Run(cfg config.Config, st *store.Store, selfNodeID nodeid.NodeID) (*HyDFSDaemon, error) {

	paths, err := storage.NewFSPaths(cfg.HydfsFileDir, cfg.LocalFileDir)
	if err != nil {
		log.Fatalf("paths: %v", err)
	}

	lg := logging.NewLogger(selfNodeID, fmt.Sprintf("%s/node.log", cfg.HydfsFileDir))
	fs := storage.NewFileStore(paths, selfNodeID, lg)

	// Context for background services (ring manager, shutdown)
	ctx, cancel := context.WithCancel(context.Background())

	selfClientSeq := uint64(0)

	// Ring manager (subscribe to membership pub/sub) + router
	var ringMgr *ring.Manager
	if st != nil {
		ringMgr = ring.NewManager(ctx, st, cfg.ReplicationFactor)
	} else {
		log.Printf("[daemon] membership store is nil; starting without ring manager (single-node).")
	}

	var httpMover *ring.HTTPMover

	var fileIndex ring.FileEnumerator = storage.NewFileEnumerator(fs)

	if ringMgr != nil {
		httpMover = &ring.HTTPMover{
			Client:        &http.Client{Timeout: 10 * time.Second},
			Config:        cfg,
			Index:         fileIndex,
			SelfClientSeq: &selfClientSeq,
			SelfClientID:  selfNodeID.NodeIDToString(),
		}
		ringMgr.SetMover(httpMover)
	}

	var router *routing.Router
	if ringMgr != nil {
		router = routing.NewRouter(ringMgr)
	}

	// HTTP server
	s := &server.HTTPServer{
		FileStore:     fs,
		Log:           lg,
		SelfNodeID:    selfNodeID,
		Router:        router,
		HTTP:          &http.Client{Timeout: 10 * time.Second},
		SelfClientSeq: &selfClientSeq,
		Config:        cfg,
	}
	mux := http.NewServeMux()
	s.Register(mux)

	httpSrv := &http.Server{
		Addr:    cfg.HydfsHTTP, // e.g., "127.0.0.1:8080"
		Handler: mux,
	}

	hydfsd := &HyDFSDaemon{
		cfg:       cfg,
		fsPath:    paths,
		fileStore: fs,
		log:       lg,
		httpSrv:   httpSrv,
		cancel:    cancel,
		ringMgr:   ringMgr,
		router:    router,
	}

	// Start HTTP in background goroutine
	go func() {
		log.Printf("HyDFS HTTP listening on %s hydfs file dir=%s", cfg.HydfsHTTP, cfg.HydfsFileDir)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	return hydfsd, nil
}

// Close gracefully stops HTTP, ring manager subscription, and background context.
func (d *HyDFSDaemon) Close() {
	// Stop accepting new HTTP conns and give in-flight up to 5s to finish
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if d.httpSrv != nil {
		_ = d.httpSrv.Shutdown(ctx)
	}

	// Stop ring manager (unsubscribe from membership)
	if d.ringMgr != nil {
		d.ringMgr.Close()
	}

	// Cancel background context (if anything else is using it)
	if d.cancel != nil {
		d.cancel()
	}

	// Optional: flush logs, sync storage state, etc.
	if d.log != nil {
		d.log.Infof("daemon closed")
	}
}
