package koth

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/scorify/scorify/pkg/config"
	"github.com/scorify/scorify/pkg/ent/minion"
	"github.com/scorify/scorify/pkg/rabbitmq/rabbitmq"
	"github.com/scorify/scorify/pkg/structs"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:     "koth",
	Short:   "Start scoring koth worker",
	Long:    "Start scoring koth worker",
	Aliases: []string{"k", "worker", "w"},
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		config.InitKoth()
	},

	Run: run,
}

var kothCheckNames []string

func init() {
	Cmd.Flags().StringArrayVar(&kothCheckNames, "check", []string{}, "Deprecated: Use --topic instead. Name of koth checks")
	Cmd.Flags().MarkDeprecated("check", "Use --topic instead")
	Cmd.Flags().StringArrayVar(&kothCheckNames, "topic", []string{}, "Name of koth checks")
}

func run(cmd *cobra.Command, args []string) {
	if len(kothCheckNames) == 0 {
		err := cmd.Help()
		if err != nil {
			logrus.WithError(err).Fatal("failed to print help")
		}

		return
	}

	ctx := cmd.Context()
	for ctx.Err() == nil {
		logrus.Info("Starting koth worker")
		if err := mainLoop(ctx); err != nil && ctx.Err() == nil {
			logrus.WithError(err).Error("koth worker stopped unexpectedly")
		}
		logrus.Info("koth worker stopped")
		timer := time.NewTimer(5 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func mainLoop(ctx context.Context) error {
	rabbitmqClient, err := rabbitmq.Client(
		config.RabbitMQ.Minion.User,
		config.RabbitMQ.Minion.Password,
	)
	if err != nil {
		return err
	}
	defer rabbitmqClient.Close()

	return kothLoop(ctx, rabbitmqClient)
}

func kothLoop(ctx context.Context, rabbitmqClient *rabbitmq.RabbitMQConnections) error {
	minionCtx, minionCancel := context.WithCancel(ctx)
	defer minionCancel()

	workerEnrollClient, err := rabbitmqClient.WorkerEnrollClient()
	if err != nil {
		return err
	}

	err = workerEnrollClient.EnrollMinion(minionCtx, minion.RoleKoth)
	_ = workerEnrollClient.Close()
	if err != nil {
		return err
	}

	heartbeatClient, err := rabbitmqClient.HeartbeatClient()
	if err != nil {
		return err
	}
	defer heartbeatClient.Close()

	workerStatusListener, err := rabbitmqClient.WorkerStatusListener(minionCtx)
	if err != nil {
		return err
	}
	defer workerStatusListener.Close()

	workerStatusChannel := workerStatusListener.Consume(minionCtx)

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	var scoringCancel context.CancelFunc
	var scoringDone chan struct{}
	startScoring := func() {
		scoringCtx, cancel := context.WithCancel(minionCtx)
		scoringCancel = cancel
		scoringDone = make(chan struct{})
		go func() {
			defer close(scoringDone)
			scoreKoth(scoringCtx, rabbitmqClient)
		}()
	}
	stopScoring := func() {
		if scoringCancel == nil {
			return
		}
		scoringCancel()
		<-scoringDone
		scoringCancel = nil
		scoringDone = nil
	}
	defer stopScoring()
	startScoring()

	for {
		select {
		case <-minionCtx.Done():
			return minionCtx.Err()
		case <-ticker.C:
			err = heartbeatClient.SendHeartbeat(minionCtx)
			if err != nil {
				logrus.WithError(err).Error("failed to send heartbeat")
				return err
			}

		case workerStatus, ok := <-workerStatusChannel:
			if !ok {
				return context.Canceled
			}
			disabled := workerStatus.Disabled(config.Minion.ID)
			if disabled {
				stopScoring()
			} else if scoringCancel == nil {
				startScoring()
			}
		case <-scoringDone:
			stopScoring()
			return fmt.Errorf("koth scoring listener stopped")
		}
	}
}

func scoreKoth(ctx context.Context, RabbitMQClient *rabbitmq.RabbitMQConnections) {
	kothTaskRequestListener, err := RabbitMQClient.KothTaskRequestListener(ctx, kothCheckNames...)
	if err != nil {
		logrus.WithError(err).Error("failed to create koth task request listener")
		return
	}
	defer kothTaskRequestListener.Close()

	kothTaskResponseClient, err := RabbitMQClient.KothTaskResponseClient()
	if err != nil {
		logrus.WithError(err).Error("failed to create koth task response client")
		return
	}
	defer kothTaskResponseClient.Close()

	for {
		// recieve task request
		taskRequest, err := kothTaskRequestListener.Consume(ctx)
		if err != nil {
			logrus.WithError(err).Error("failed to consume task request")
			return
		}

		// read file
		file, err := os.Open(taskRequest.Filename)
		var content []byte
		if err == nil {
			content, err = io.ReadAll(io.LimitReader(file, 1<<20+1))
			closeErr := file.Close()
			if err == nil {
				err = closeErr
			}
			if err == nil && len(content) > 1<<20 {
				err = fmt.Errorf("koth file exceeds 1 MiB check limit")
			}
		}
		if err != nil {
			logrus.WithError(err).Error("failed to read file")
			err = kothTaskResponseClient.SubmitKothTaskResponse(ctx, &structs.KothTaskResponse{
				StatusID: taskRequest.StatusID,
				MinionID: config.Minion.ID,
				Error:    err.Error(),
			})
			if err != nil {
				logrus.WithError(err).Error("failed to submit koth task response")
			}
			continue
		}

		// submit task response
		err = kothTaskResponseClient.SubmitKothTaskResponse(ctx, &structs.KothTaskResponse{
			StatusID: taskRequest.StatusID,
			MinionID: config.Minion.ID,
			Content:  string(bytes.TrimSpace(content)),
		})
		if err != nil {
			logrus.WithError(err).Error("failed to submit koth task response")
		}
	}
}
