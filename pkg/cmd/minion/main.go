package minion

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/scorify/scorify/pkg/checks"
	"github.com/scorify/scorify/pkg/config"
	"github.com/scorify/scorify/pkg/ent/minion"
	"github.com/scorify/scorify/pkg/ent/status"
	"github.com/scorify/scorify/pkg/rabbitmq/rabbitmq"
	"github.com/scorify/scorify/pkg/structs"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:     "minion",
	Short:   "Start scoring minion worker",
	Long:    "Start scoring minion worker",
	Aliases: []string{"m", "worker", "w"},
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		config.InitMinion()
	},

	Run: run,
}

func run(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()
	for ctx.Err() == nil {
		logrus.Info("Starting minion worker")
		mainLoop(ctx)
		logrus.Info("minion worker stopped")
		timer := time.NewTimer(5 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func mainLoop(ctx context.Context) {
	rabbitmqClient, err := rabbitmq.Client(
		config.RabbitMQ.Minion.User,
		config.RabbitMQ.Minion.Password,
	)
	if err != nil {
		logrus.WithError(err).Error("failed to create rabbitmq client")
		return
	}
	defer rabbitmqClient.Close()

	var backOff atomic.Int64
	backOff.Store(int64(time.Second))
	heartbeatSuccess := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Reset backoff on successful heartbeat
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-heartbeatSuccess:
				backOff.Store(int64(time.Second))
			}
		}
	}()

	// Run minion loop, retry with backoff on failure
	// Exit mainLoop when backoff reaches max (connection likely unhealthy)
	if err := minionLoop(ctx, rabbitmqClient, heartbeatSuccess); err != nil && ctx.Err() == nil {
		logrus.WithError(err).Warn("minion loop stopped")
	}
	for ctx.Err() == nil {
		currentBackOff := time.Duration(backOff.Load())
		if currentBackOff >= time.Minute {
			logrus.Warn("max backoff reached, reconnecting to rabbitmq")
			return
		}

		timer := time.NewTimer(currentBackOff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if err := minionLoop(ctx, rabbitmqClient, heartbeatSuccess); err != nil && ctx.Err() == nil {
			logrus.WithError(err).Warn("minion loop stopped")
		}

		newBackOff := backOff.Load() * 2
		backOff.Store(min(newBackOff, int64(time.Minute)))
	}
}

func minionLoop(ctx context.Context, rabbitmqClient *rabbitmq.RabbitMQConnections, heartbeatSuccess chan struct{}) error {
	minionCtx, minionCancel := context.WithCancel(ctx)
	defer minionCancel()

	workerEnrollClient, err := rabbitmqClient.WorkerEnrollClient()
	if err != nil {
		return fmt.Errorf("create worker enroll client: %w", err)
	}

	err = workerEnrollClient.EnrollMinion(minionCtx, minion.RoleService)
	_ = workerEnrollClient.Close()
	if err != nil {
		return fmt.Errorf("enroll minion: %w", err)
	}

	heartbeatClient, err := rabbitmqClient.HeartbeatClient()
	if err != nil {
		return fmt.Errorf("create heartbeat client: %w", err)
	}
	defer heartbeatClient.Close()

	workerStatusListener, err := rabbitmqClient.WorkerStatusListener(minionCtx)
	if err != nil {
		return fmt.Errorf("create worker status listener: %w", err)
	}
	defer workerStatusListener.Close()

	workerStatusChannel := workerStatusListener.Consume(minionCtx)

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	err = heartbeatClient.SendHeartbeat(minionCtx)
	if err != nil {
		return fmt.Errorf("send heartbeat: %w", err)
	}

	select {
	case heartbeatSuccess <- struct{}{}:
	default:
	}

	var scoringCancel context.CancelFunc
	var scoringDone chan struct{}
	startScoring := func() {
		scoringCtx, cancel := context.WithCancel(minionCtx)
		scoringCancel = cancel
		scoringDone = make(chan struct{})
		go func() {
			defer close(scoringDone)
			score(scoringCtx, rabbitmqClient)
		}()
	}
	stopScoring := func() {
		if scoringCancel != nil {
			scoringCancel()
			<-scoringDone
			scoringCancel = nil
			scoringDone = nil
		}
	}
	defer stopScoring()
	startScoring()

	for {
		select {
		case <-minionCtx.Done():
			return minionCtx.Err()
		case <-ticker.C:
			err := heartbeatClient.SendHeartbeat(minionCtx)
			if err != nil {
				return fmt.Errorf("send heartbeat: %w", err)
			}
			select {
			case heartbeatSuccess <- struct{}{}:
			default:
			}

		case workerStatus, ok := <-workerStatusChannel:
			if !ok {
				return fmt.Errorf("worker status channel closed")
			}
			disabled := workerStatus.Disabled(config.Minion.ID)

			if disabled {
				stopScoring()
			} else if scoringCancel == nil {
				startScoring()
			}
		case <-scoringDone:
			stopScoring()
			return fmt.Errorf("scoring listener stopped")
		}
	}
}

func score(ctx context.Context, rabbitmqClient *rabbitmq.RabbitMQConnections) {
	taskRequestListener, err := rabbitmqClient.TaskRequestListener(ctx)
	if err != nil {
		logrus.WithError(err).Error("failed to create task request listener")
		return
	}
	defer taskRequestListener.Close()

	taskResponseClient, err := rabbitmqClient.TaskResponseClient()
	if err != nil {
		logrus.WithError(err).Error("failed to create task response client")
		return
	}
	defer taskResponseClient.Close()

	// Checks are network-bound, so allow modest concurrency while keeping each
	// minion's memory, sockets, and goroutine count bounded.
	maxConcurrentChecks := min(max(runtime.NumCPU()*2, 4), 32)
	semaphore := make(chan struct{}, maxConcurrentChecks)
	var checksWg sync.WaitGroup
	defer checksWg.Wait()

	for {
		// recieved score task
		task, err := taskRequestListener.Consume(ctx)
		if err != nil {
			logrus.WithError(err).Error("failed to consume task request")
			return
		}

		checkDeadline := time.Now().Add(time.Duration(float64(config.Interval) * 0.8))
		submissionDeadline := time.Now().Add(time.Duration(float64(config.Interval) * 0.85))

		// check if source exists
		check, ok := checks.Checks[task.SourceName]
		if !ok {
			logrus.WithField("source", task.SourceName).
				Error("source not found")

			err = taskResponseClient.SubmitTaskResponse(
				ctx,
				&structs.TaskResponse{
					StatusID: task.StatusID,
					MinionID: config.Minion.ID,
					Status:   status.StatusDown,
					Error:    "source not found",
				},
			)
			if err != nil {
				logrus.WithError(err).Error("encountered error while submitting score task")
				return
			}
			continue
		}

		select {
		case <-ctx.Done():
			return
		case semaphore <- struct{}{}:
		}

		// run check
		checksWg.Add(1)
		go func(checkDeadline time.Time, submissionDeadline time.Time, statusID uuid.UUID, check checks.Check, task_config string) {
			defer checksWg.Done()
			defer func() { <-semaphore }()
			checkCtx, checkCancel := context.WithDeadline(ctx, checkDeadline)
			submissionCtx, submissionCancel := context.WithDeadline(ctx, submissionDeadline)
			defer checkCancel()
			defer submissionCancel()

			// run check
			err := check.Func(checkCtx, task_config)

			// submit score task
			if err != nil {
				err = taskResponseClient.SubmitTaskResponse(
					submissionCtx,
					&structs.TaskResponse{
						StatusID: statusID,
						MinionID: config.Minion.ID,
						Status:   status.StatusDown,
						Error:    err.Error(),
					},
				)
			} else {
				err = taskResponseClient.SubmitTaskResponse(
					submissionCtx,
					&structs.TaskResponse{
						StatusID: statusID,
						MinionID: config.Minion.ID,
						Status:   status.StatusUp,
					},
				)
			}

			// log error if submission failed
			if err != nil {
				logrus.WithError(err).Error("encountered error while submitting score task")
			}
		}(checkDeadline, submissionDeadline, task.StatusID, check, task.Config)
	}
}
