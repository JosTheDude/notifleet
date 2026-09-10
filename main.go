package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func run() error {
	path := flag.String("config", "config.toml", "path to TOML configuration")
	check := flag.Bool("check", false, "validate configuration and exit without opening the queue or sending messages")
	healthcheck := flag.Bool("healthcheck", false, "check the local running service and exit")
	flag.Parse()
	c, err := loadConfig(*path)
	if err != nil {
		return err
	}
	if *check {
		fmt.Println("configuration valid")
		return nil
	}
	if *healthcheck {
		client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{Proxy: nil}}
		host, port, err := net.SplitHostPort(c.Listen)
		if err != nil {
			return errors.New("invalid listen address")
		}
		if host == "" || host == "0.0.0.0" {
			host = "127.0.0.1"
		} else if host == "::" {
			host = "::1"
		}
		resp, err := client.Get("http://" + net.JoinHostPort(host, port) + "/healthz")
		if err != nil {
			return errors.New("health check failed")
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			return errors.New("service unhealthy")
		}
		return nil
	}
	q, err := openQueue(c)
	if err != nil {
		return err
	}
	defer q.close()
	listener, err := net.Listen("tcp", c.Listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	server := &http.Server{Addr: c.Listen, Handler: newHandler(c, q), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	workerDone, serverDone, feedsDone := make(chan error, 1), make(chan error, 1), make(chan struct{})
	go func() { workerDone <- q.run(ctx) }()
	go func() { serverDone <- server.Serve(listener) }()
	go func() { runFeeds(ctx, c, q); close(feedsDone) }()
	slog.Info("Notifleet starting", "listen", c.Listen, "destinations", len(c.Destinations), "routes", len(c.Routes), "feeds", len(c.Feeds))
	workerStopped := false
	select {
	case <-ctx.Done():
	case err = <-workerDone:
		workerStopped = true
	case err = <-serverDone:
	}
	shutdownCtx, stop := context.WithTimeout(context.Background(), 20*time.Second)
	defer stop()
	if shutdownErr := server.Shutdown(shutdownCtx); shutdownErr != nil {
		server.Close()
		if err == nil {
			err = shutdownErr
		}
	}
	cancel()
	if !workerStopped {
		if workerErr := <-workerDone; err == nil {
			err = workerErr
		}
	}
	<-feedsDone
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("Notifleet stopped", "error", err.Error())
		os.Exit(1)
	}
}
