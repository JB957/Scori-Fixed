package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"github.com/scorify/scorify/pkg/cache"
	"github.com/scorify/scorify/pkg/config"
	"github.com/scorify/scorify/pkg/ent"
	"github.com/scorify/scorify/pkg/ent/minion"
	"github.com/scorify/scorify/pkg/structs"
	"github.com/sirupsen/logrus"
)

type RabbitMQConnections struct {
	Heartbeat        *amqp.Connection
	TaskRequest      *amqp.Connection
	TaskResponse     *amqp.Connection
	WorkerEnroll     *amqp.Connection
	WorkerStatus     *amqp.Connection
	KothTaskResponse *amqp.Connection
	KothTaskRequest  *amqp.Connection
}

func (r *RabbitMQConnections) Close() error {
	var closeErrors []error
	for _, conn := range []*amqp.Connection{
		r.Heartbeat, r.TaskRequest, r.TaskResponse, r.WorkerEnroll,
		r.WorkerStatus, r.KothTaskResponse, r.KothTaskRequest,
	} {
		if conn != nil {
			if err := conn.Close(); err != nil {
				closeErrors = append(closeErrors, err)
			}
		}
	}
	return errors.Join(closeErrors...)
}

func openConnection(vhost string, username string, password string) (*amqp.Connection, error) {
	connStr := (&url.URL{
		Scheme: "amqp",
		User:   url.UserPassword(username, password),
		Host:   net.JoinHostPort(config.RabbitMQ.Host, fmt.Sprint(config.RabbitMQ.Port)),
		Path:   "/" + vhost,
	}).String()
	conn, err := amqp.DialConfig(connStr, amqp.Config{
		Heartbeat: 10 * time.Second,
		Dial:      amqp.DefaultDial(10 * time.Second),
	})
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func Client(username string, password string) (*RabbitMQConnections, error) {
	opened := make([]*amqp.Connection, 0, 7)
	cleanup := true
	defer func() {
		if cleanup {
			for _, conn := range opened {
				_ = conn.Close()
			}
		}
	}()
	open := func(vhost string) (*amqp.Connection, error) {
		conn, err := openConnection(vhost, username, password)
		if err == nil {
			opened = append(opened, conn)
		}
		return conn, err
	}
	heartbeatConn, err := open(HeartbeatVhost)
	if err != nil {
		return nil, err
	}

	taskRequestsConn, err := open(TaskRequestVhost)
	if err != nil {
		return nil, err
	}

	taskResponsesConn, err := open(TaskResponseVhost)
	if err != nil {
		return nil, err
	}

	workerEnrollConn, err := open(WorkerEnrollVhost)
	if err != nil {
		return nil, err
	}

	workerStatusConn, err := open(WorkerStatusVhost)
	if err != nil {
		return nil, err
	}

	kothTaskRequestConn, err := open(KothTaskRequestVhost)
	if err != nil {
		return nil, err
	}

	kothTaskResponseConn, err := open(KothTaskResponseVhost)
	if err != nil {
		return nil, err
	}

	cleanup = false
	return &RabbitMQConnections{
		Heartbeat:        heartbeatConn,
		TaskRequest:      taskRequestsConn,
		TaskResponse:     taskResponsesConn,
		WorkerEnroll:     workerEnrollConn,
		WorkerStatus:     workerStatusConn,
		KothTaskRequest:  kothTaskRequestConn,
		KothTaskResponse: kothTaskResponseConn,
	}, nil
}

func Serve(
	ctx context.Context,
	taskRequestChan <-chan *structs.TaskRequest,
	taskResponseChan chan<- *structs.TaskResponse,
	workerStatusChan <-chan *structs.WorkerStatus,
	kothStatusRequestChan <-chan *structs.KothTaskRequestBundle,
	kothStatusResponseChan chan<- *structs.KothTaskResponse,
	redisClient *redis.Client,
	entClient *ent.Client,
) error {
	rabbitmqClient, err := Client(config.RabbitMQ.Server.User, config.RabbitMQ.Server.Password)
	if err != nil {
		return err
	}
	defer rabbitmqClient.Close()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	logrus.Info("Connected to RabbitMQ server")
	var workers sync.WaitGroup
	launch := func(fn func()) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer cancel()
			fn()
		}()
	}

	launch(func() {
		heartbeatListener, err := rabbitmqClient.HeartbeatListener(ctx)
		if err != nil {
			logrus.WithError(err).Error("failed to create heartbeat listener")
			return
		}
		defer heartbeatListener.Close()

		for {
			heartbeat, err := heartbeatListener.Consume(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				logrus.WithError(err).Error("failed to consume heartbeat")
				return
			}

			heartbeat.Timestamp = time.Now()

			err = cache.SetMinionHeartbeat(ctx, heartbeat.MinionID, redisClient, &structs.Heartbeat{
				MinionID:    heartbeat.MinionID,
				Timestamp:   heartbeat.Timestamp,
				MemoryUsage: heartbeat.MemoryUsage,
				MemoryTotal: heartbeat.MemoryTotal,
				CPUUsage:    heartbeat.CPUUsage,
				Goroutines:  heartbeat.Goroutines,
			})
			if err != nil {
				logrus.WithError(err).Error("failed to set minion metrics")
			}

			_, err = cache.PublishMinionHeartbeat(ctx, redisClient, &structs.Heartbeat{
				MinionID:    heartbeat.MinionID,
				Timestamp:   heartbeat.Timestamp,
				MemoryUsage: heartbeat.MemoryUsage,
				MemoryTotal: heartbeat.MemoryTotal,
				CPUUsage:    heartbeat.CPUUsage,
				Goroutines:  heartbeat.Goroutines,
			})
			if err != nil {
				logrus.WithError(err).Error("failed to publish minion metrics")
			}
		}
	})

	launch(func() {
		taskRequestClient, err := rabbitmqClient.TaskRequestClient()
		if err != nil {
			logrus.WithError(err).Error("failed to create task request client")
			return
		}
		defer taskRequestClient.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case taskRequest := <-taskRequestChan:
				err := taskRequestClient.Publish(ctx, taskRequest)
				if err != nil {
					logrus.WithError(err).Error("failed to send task request")
					return
				}
			}
		}
	})

	launch(func() {
		taskResponseListener, err := rabbitmqClient.TaskResponseListener(ctx)
		if err != nil {
			logrus.WithError(err).Error("failed to create task response listener")
			return
		}
		defer taskResponseListener.Close()

		for {
			taskResponse, err := taskResponseListener.Consume(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				logrus.WithError(err).Error("failed to consume task response")
				return
			}

			select {
			case <-ctx.Done():
				return
			case taskResponseChan <- taskResponse:
			}
		}
	})

	launch(func() {
		workerEnrollListener, err := rabbitmqClient.WorkerEnrollListener(ctx)
		if err != nil {
			logrus.WithError(err).Error("failed to create worker enroll listener")
			return
		}
		defer workerEnrollListener.Close()

		for {
			workerEnroll, err := workerEnrollListener.Consume(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				logrus.WithError(err).Error("failed to consume worker enroll")
				return
			}

			exists, err := entClient.Minion.
				Query().
				Where(
					minion.IDEQ(
						workerEnroll.MinionID,
					),
				).
				Exist(ctx)
			if err != nil {
				logrus.WithError(err).Error("failed to get minion")
			}

			if exists {
				_, err = entClient.Minion.
					UpdateOneID(workerEnroll.MinionID).
					SetIP(workerEnroll.Hostname).
					Save(ctx)
				if err != nil {
					logrus.WithError(err).Error("failed to update minion")
				}
			} else {
				_, err = entClient.Minion.
					Create().
					SetID(workerEnroll.MinionID).
					SetName(workerEnroll.Hostname).
					SetIP(workerEnroll.Hostname).
					SetRole(workerEnroll.Role).
					Save(ctx)
				if err != nil {
					logrus.WithError(err).Error("failed to create minion")
				}
			}
		}
	})

	launch(func() {
		workerStatusClient, err := rabbitmqClient.WorkerStatusClient()
		if err != nil {
			logrus.WithError(err).Error("failed to create worker status client")
			return
		}
		defer workerStatusClient.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case workerStatus := <-workerStatusChan:
				err := workerStatusClient.Publish(ctx, workerStatus)
				if err != nil {
					logrus.WithError(err).Error("failed to send worker status")
					return
				}
			}
		}
	})

	launch(func() {
		kothTaskRequestClient, err := rabbitmqClient.KothTaskRequestClient()
		if err != nil {
			logrus.WithError(err).Error("failed to create koth task request client")
			return
		}
		defer kothTaskRequestClient.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case kothTaskRequest := <-kothStatusRequestChan:
				err := kothTaskRequestClient.Publish(ctx, kothTaskRequest.RoutingKey, &kothTaskRequest.KothTaskRequest)
				if err != nil {
					logrus.WithError(err).Error("failed to send koth task request")
					return
				}
			}
		}
	})

	launch(func() {
		kothTaskResponseListener, err := rabbitmqClient.KothTaskResponseListener(ctx)
		if err != nil {
			logrus.WithError(err).Error("failed to create koth task response listener")
			return
		}
		defer kothTaskResponseListener.Close()

		for {
			kothTaskResponse, err := kothTaskResponseListener.Consume(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				logrus.WithError(err).Error("failed to consume koth task response")
				return
			}

			select {
			case <-ctx.Done():
				return
			case kothStatusResponseChan <- kothTaskResponse:
			}
		}
	})

	<-ctx.Done()
	workers.Wait()

	return nil
}
