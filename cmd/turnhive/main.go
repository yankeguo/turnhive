package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yankeguo/ironhive"
	"github.com/yankeguo/turnhive/config"
	"github.com/yankeguo/turnhive/controller"
	"github.com/yankeguo/turnhive/registry"
	"github.com/yankeguo/turnhive/storage"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	configPath := flag.String("config", "config.yml", "path to the configuration file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	log.Printf("config loaded: node id=%q advertise=%q, s3 bucket=%q prefix=%q, etcd endpoints=%v",
		cfg.Node.ID, cfg.Node.Advertise, cfg.S3.Bucket, cfg.S3.Prefix, cfg.Etcd.Endpoints)

	etcdClient, err := newEtcdClient(cfg)
	if err != nil {
		log.Fatalf("connect etcd: %v", err)
	}
	defer etcdClient.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg := registry.New(etcdClient, cfg.Etcd.Prefix, cfg.Node.ID, cfg.Node.Advertise, time.Duration(cfg.Etcd.LeaseTTL))
	if err = reg.RegisterNode(ctx); err != nil {
		log.Fatalf("register node: %v", err)
	}
	log.Printf("node registered in etcd with lease TTL %s", time.Duration(cfg.Etcd.LeaseTTL))

	mux := http.NewServeMux()
	store, err := storage.New(cfg.S3)
	if err != nil {
		log.Fatalf("connect s3: %v", err)
	}
	ihClient := ironhive.NewClient(cfg.Ironhive.URL)
	controller.New(cfg.Node.ID, reg, ihClient, store, time.Duration(cfg.Ironhive.Lease)).RegisterRoutes(mux)

	srv := &http.Server{
		Addr:    cfg.Listen,
		Handler: mux,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("listening on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		if err != nil {
			log.Fatalf("server error: %v", err)
		}
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("graceful shutdown failed: %v", err)
	}
	// Revoke the node lease so the node record and all of its session
	// records disappear from etcd immediately.
	if err := reg.Close(shutdownCtx); err != nil {
		log.Printf("deregister node: %v", err)
	}
	log.Println("server stopped")
}

// newEtcdClient builds an etcd client from the configuration, including
// optional authentication and TLS.
func newEtcdClient(cfg *config.Config) (*clientv3.Client, error) {
	etcdCfg := clientv3.Config{
		Endpoints:   cfg.Etcd.Endpoints,
		DialTimeout: time.Duration(cfg.Etcd.DialTimeout),
		Username:    cfg.Etcd.Username,
		Password:    cfg.Etcd.Password,
	}
	if cfg.Etcd.TLS.CertFile != "" || cfg.Etcd.TLS.CAFile != "" {
		tlsInfo := transport.TLSInfo{
			CertFile:      cfg.Etcd.TLS.CertFile,
			KeyFile:       cfg.Etcd.TLS.KeyFile,
			TrustedCAFile: cfg.Etcd.TLS.CAFile,
		}
		tlsConfig, err := tlsInfo.ClientConfig()
		if err != nil {
			return nil, err
		}
		etcdCfg.TLS = tlsConfig
	}
	return clientv3.New(etcdCfg)
}
