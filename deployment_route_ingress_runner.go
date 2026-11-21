
package main

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	deploymentRouteIngressDefaultWorkerID = "nexvarin-ingress"
	deploymentRouteIngressClaimLimit      = 1
	deploymentRouteIngressLockReleaseTime = 2 * time.Second
)

type deploymentRouteIngressRunner struct {
	Pool          *pgxpool.Pool
	WorkerID      string
	ServerName    string
	Reconciler    deploymentRouteReconciler
	LeaseDuration time.Duration
	PollInterval  time.Duration
}

// Run continuously claims and reconciles one due route at a time. Mutations
// for the configured Caddy server are serialized across ingress processes by
// a PostgreSQL session advisory lock. The lock is acquired before claiming so
// a route never spends its lease waiting for access to the shared Caddy scope.
func (runner deploymentRouteIngressRunner) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("deployment route ingress context is required")
	}
	if err := runner.validate(); err != nil {
		return err
	}

	for {
		processed, err := runner.RunOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		if processed {
			continue
		}

		timer := time.NewTimer(runner.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

// RunOnce processes at most one due route. It returns false when no route is
// due or another ingress process currently owns this Caddy configuration
// scope. The Caddy scope lock is session-level and does not hold PostgreSQL
// row locks while the reconciler communicates with Caddy.
func (runner deploymentRouteIngressRunner) RunOnce(
	ctx context.Context,
) (bool, error) {
	if ctx == nil {
		return false, errors.New("deployment route ingress context is required")
	}
	if err := runner.validate(); err != nil {
		return false, err
	}

	connection, err := runner.Pool.Acquire(ctx)
	if err != nil {
		return false, fmt.Errorf(
			"acquire database connection for Caddy scope lock: %w",
			err,
		)
	}

	lockKey := deploymentRouteCaddyScopeAdvisoryLockKey(
		runner.ServerName,
	)

	var locked bool
	if err := connection.QueryRow(
		ctx,
		"SELECT pg_try_advisory_lock($1::bigint)",
		lockKey,
	).Scan(&locked); err != nil {
		// The server may have granted the session lock before the response
		// was lost. Destroy this physical connection so PostgreSQL releases
		// any session-level lock instead of returning it to the pool.
		discardDeploymentRouteIngressLockConnection(connection)
		return false, fmt.Errorf(
			"acquire Caddy configuration scope lock: %w",
			err,
		)
	}

	if !locked {
		connection.Release()
		return false, nil
	}

	processed, processErr := runner.processOneClaim(ctx)
	unlockErr := releaseDeploymentRouteIngressScopeLock(
		connection,
		lockKey,
	)
	if processErr != nil && unlockErr != nil {
		return processed, errors.Join(processErr, unlockErr)
	}
	if processErr != nil {
		return processed, processErr
	}
	if unlockErr != nil {
		return processed, unlockErr
	}

	return processed, nil
}

func (runner deploymentRouteIngressRunner) processOneClaim(
	ctx context.Context,
) (bool, error) {
	claims, err := claimDeploymentRoutes(
		ctx,
		runner.Pool,
		runner.WorkerID,
		deploymentRouteIngressClaimLimit,
		runner.LeaseDuration,
	)
	if err != nil {
		if ctx.Err() != nil {
			return false, nil
		}
		return false, fmt.Errorf("claim due deployment route: %w", err)
	}
	if len(claims) == 0 {
		return false, nil
	}
	if len(claims) != deploymentRouteIngressClaimLimit {
		return false, errors.New(
			"deployment route claim returned an unexpected number of routes",
		)
	}

	claim := claims[0]
	if err := runner.Reconciler.reconcileClaim(ctx, claim); err != nil {
		if ctx.Err() != nil {
			// A claim made immediately before shutdown should not remain
			// leased until expiry. The reconciler uses a bounded background
			// recovery context when the processing context is cancelled.
			if retryErr := runner.Reconciler.scheduleRetry(
				ctx,
				claim,
				0,
				"controller_cancelled",
			); retryErr != nil {
				return true, errors.Join(
					fmt.Errorf("reconcile deployment route during shutdown: %w", err),
					retryErr,
				)
			}
			return true, nil
		}

		return true, fmt.Errorf(
			"reconcile deployment route %s: %w",
			claim.Route.ID,
			err,
		)
	}

	return true, nil
}

func (runner deploymentRouteIngressRunner) validate() error {
	if runner.Pool == nil {
		return errors.New("deployment route ingress database pool is required")
	}
	if runner.Pool.Config().MaxConns < 2 {
		return errors.New(
			"deployment route ingress database pool must allow at least two connections",
		)
	}
	if runner.Reconciler.Pool == nil || runner.Reconciler.Pool != runner.Pool {
		return errors.New(
			"deployment route ingress runner and reconciler must share the same database pool",
		)
	}
	if runner.Reconciler.Adapter == nil {
		return errors.New("deployment route ingress Caddy adapter is required")
	}
	if _, err := normalizeOperationWorkerID(runner.WorkerID); err != nil {
		return fmt.Errorf("deployment route ingress worker ID: %w", err)
	}
	if _, err := caddyAdminServerRoutesPath(runner.ServerName); err != nil {
		return fmt.Errorf("deployment route ingress Caddy server: %w", err)
	}
	if _, err := normalizeDeploymentRoutePreviewSuffix(
		runner.Reconciler.PreviewSuffix,
	); err != nil {
		return fmt.Errorf("deployment route ingress preview suffix: %w", err)
	}
	if runner.LeaseDuration < time.Microsecond {
		return errors.New(
			"deployment route ingress lease duration must be at least one microsecond",
		)
	}
	if runner.PollInterval <= 0 {
		return errors.New("deployment route ingress poll interval must be positive")
	}

	return nil
}

func deploymentRouteCaddyScopeAdvisoryLockKey(serverName string) int64 {
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte("nexvarin:caddy-server:" + serverName))
	return int64(hasher.Sum64())
}

func releaseDeploymentRouteIngressScopeLock(
	connection *pgxpool.Conn,
	lockKey int64,
) error {
	if connection == nil {
		return errors.New("Caddy configuration scope lock connection is required")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		deploymentRouteIngressLockReleaseTime,
	)
	defer cancel()

	var unlocked bool
	err := connection.QueryRow(
		ctx,
		"SELECT pg_advisory_unlock($1::bigint)",
		lockKey,
	).Scan(&unlocked)
	if err != nil {
		discardDeploymentRouteIngressLockConnection(connection)
		return fmt.Errorf("release Caddy configuration scope lock: %w", err)
	}

	connection.Release()
	if !unlocked {
		return errors.New(
			"Caddy configuration scope lock was not held by its database session",
		)
	}

	return nil
}

func discardDeploymentRouteIngressLockConnection(
	connection *pgxpool.Conn,
) {
	if connection == nil {
		return
	}

	physicalConnection := connection.Hijack()
	_ = physicalConnection.Close(context.Background())
}
