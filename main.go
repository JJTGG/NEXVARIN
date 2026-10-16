package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultListenAddress = "127.0.0.1:8080"
	defaultWorkerID      = "nexvarin-worker"

	databasePingTimeout = 2 * time.Second
	shutdownTimeout     = 10 * time.Second
	maxDatabaseConns    = 4

	operationWorkerLeaseDuration     = 2 * time.Minute
	operationWorkerHeartbeatInterval = 15 * time.Second
	operationWorkerPollInterval      = time.Second
)

type healthResponse struct {
	Status string `json:"status"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	if err := run(); err != nil {
		slog.Error("NEXVARIN process stopped with an error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	processKind, err := operationProcessKindFromEnvironment()
	if err != nil {
		return err
	}

	var pool *pgxpool.Pool

	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		if processKind == "worker" {
			return errors.New("DATABASE_URL is required in worker mode")
		}

		slog.Warn(
			"DATABASE_URL is not set; readiness will report unavailable",
		)
	} else {
		config, err := pgxpool.ParseConfig(databaseURL)
		if err != nil {
			return errors.New(
				"could not parse DATABASE_URL; its value was not logged",
			)
		}

		config.MaxConns = maxDatabaseConns
		config.MinConns = 0
		config.MaxConnIdleTime = 5 * time.Minute
		config.MaxConnLifetime = time.Hour

		pool, err = pgxpool.NewWithConfig(
			context.Background(),
			config,
		)
		if err != nil {
			return errors.New(
				"could not configure the database connection pool",
			)
		}
		defer pool.Close()
	}

	if processKind == "worker" {
		return runOperationWorkerProcess(pool)
	}

	return runControlAPIProcess(pool)
}

func operationProcessKindFromEnvironment() (string, error) {
	processKind := strings.ToLower(
		strings.TrimSpace(os.Getenv("NEXVARIN_PROCESS")),
	)

	if processKind == "" {
		processKind = "api"
	}

	switch processKind {
	case "api", "worker":
		return processKind, nil
	default:
		return "", errors.New(
			"NEXVARIN_PROCESS must be either api or worker",
		)
	}
}

func runControlAPIProcess(pool *pgxpool.Pool) error {
	listenAddress := strings.TrimSpace(
		os.Getenv("NEXVARIN_LISTEN_ADDR"),
	)
	if listenAddress == "" {
		listenAddress = defaultListenAddress
	}

	operationAuthenticator, err :=
		loadOperationHTTPAuthenticatorFromEnvironment()
	if err != nil {
		return fmt.Errorf(
			"configure operation authentication: %w",
			err,
		)
	}

	operationAuthorizer := newPostgresOperationCreationAuthorizer(pool)

	server := &http.Server{
		Addr: listenAddress,
		Handler: newHTTPHandler(
			pool,
			operationAuthenticator,
			operationAuthorizer,
		),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	signalContext, stopSignals := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stopSignals()

	serverErrors := make(chan error, 1)

	go func() {
		slog.Info(
			"control API listening",
			"address",
			listenAddress,
			"process",
			"api",
		)

		err := server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrors <- err
			return
		}

		serverErrors <- nil
	}()

	select {
	case err := <-serverErrors:
		if err != nil {
			return fmt.Errorf("serve HTTP API: %w", err)
		}
		return nil

	case <-signalContext.Done():
		slog.Info("control API shutdown signal received")

		shutdownContext, cancel := context.WithTimeout(
			context.Background(),
			shutdownTimeout,
		)
		defer cancel()

		if err := server.Shutdown(shutdownContext); err != nil {
			_ = server.Close()
			return fmt.Errorf("shut down HTTP API: %w", err)
		}

		if err := <-serverErrors; err != nil {
			return fmt.Errorf(
				"HTTP API returned an error during shutdown: %w",
				err,
			)
		}

		slog.Info("control API stopped cleanly")
		return nil
	}
}

func runOperationWorkerProcess(pool *pgxpool.Pool) error {
	if pool == nil {
		return errors.New("database pool is required in worker mode")
	}

	workerID := strings.TrimSpace(
		os.Getenv("NEXVARIN_WORKER_ID"),
	)
	if workerID == "" {
		workerID = defaultWorkerID
	}

	workerID, err := normalizeOperationWorkerID(workerID)
	if err != nil {
		return fmt.Errorf("configure worker ID: %w", err)
	}

	buildctlPath := strings.TrimSpace(
		os.Getenv("NEXVARIN_BUILDKIT_PATH"),
	)
	if buildctlPath == "" {
		buildctlPath = "buildctl"
	}

	resolvedBuildctl, err := exec.LookPath(buildctlPath)
	if err != nil {
		return errors.New(
			"BuildKit client executable was not found; configure NEXVARIN_BUILDKIT_PATH",
		)
	}

	buildkitAddress, err := validateOperationWorkerBuildkitAddress(
		os.Getenv("NEXVARIN_BUILDKIT_ADDRESS"),
	)
	if err != nil {
		return fmt.Errorf("configure BuildKit address: %w", err)
	}

	artifactDirectory, err := prepareOperationWorkerArtifactDirectory(
		os.Getenv("NEXVARIN_ARTIFACT_DIRECTORY"),
	)
	if err != nil {
		return fmt.Errorf("configure artifact storage: %w", err)
	}

	buildExecutor := operationWorkerBuildExecutor{
		Pool:              pool,
		BuildctlPath:      resolvedBuildctl,
		BuildkitAddress:   buildkitAddress,
		ArtifactDirectory: artifactDirectory,
		BuildTimeout:      operationWorkerBuildTimeout,
	}

	executor := newOperationExecutorMux(
		map[operationType]operationExecutor{
			operationTypeBuild: buildExecutor,
		},
	)

	worker := operationWorkerRunner{
		Pool:              pool,
		WorkerID:          workerID,
		Executor:          executor,
		LeaseDuration:     operationWorkerLeaseDuration,
		HeartbeatInterval: operationWorkerHeartbeatInterval,
		PollInterval:      operationWorkerPollInterval,
	}

	signalContext, stopSignals := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stopSignals()

	slog.Info(
		"operation worker starting",
		"worker_id",
		workerID,
		"poll_interval",
		operationWorkerPollInterval.String(),
		"lease_duration",
		operationWorkerLeaseDuration.String(),
	)

	if err := worker.Run(signalContext); err != nil {
		return fmt.Errorf("run operation worker: %w", err)
	}

	slog.Info("operation worker stopped cleanly")
	return nil
}

func newHTTPHandler(
	pool *pgxpool.Pool,
	operationAuthenticator operationHTTPAuthenticator,
	operationAuthorizer operationCreationAuthorizer,
) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health/live", func(
		w http.ResponseWriter,
		_ *http.Request,
	) {
		writeHealth(w, http.StatusOK, "ok")
	})

	mux.Handle("GET /health/ready", readinessHandler(pool))

	mux.Handle(
		"POST /v1/projects/{projectId}/repositories/{repositoryId}/operations",
		createRepositoryOperationHandler(
			pool,
			operationAuthenticator,
			operationAuthorizer,
		),
	)

	mux.Handle(
		"GET /v1/projects/{projectId}/repositories/{repositoryId}/operations",
		listRepositoryOperationsHandler(
			pool,
			operationAuthenticator,
			newPostgresOperationListAuthorizer(pool),
		),
	)

	mux.Handle(
		"GET /v1/operations/{operationId}",
		getOperationHandler(
			pool,
			operationAuthenticator,
			newPostgresOperationReadAuthorizer(pool),
		),
	)

	return mux
}

func readinessHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if pool == nil {
			writeHealth(
				w,
				http.StatusServiceUnavailable,
				"unavailable",
			)
			return
		}

		ctx, cancel := context.WithTimeout(
			r.Context(),
			databasePingTimeout,
		)
		defer cancel()

		if err := pool.Ping(ctx); err != nil {
			writeHealth(
				w,
				http.StatusServiceUnavailable,
				"unavailable",
			)
			return
		}

		writeHealth(w, http.StatusOK, "ok")
	}
}

func writeHealth(
	w http.ResponseWriter,
	statusCode int,
	status string,
) {
	w.Header().Set(
		"Content-Type",
		"application/json; charset=utf-8",
	)
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(statusCode)

	if err := json.NewEncoder(w).Encode(
		healthResponse{Status: status},
	); err != nil {
		slog.Warn("could not write health response")
	}
}
