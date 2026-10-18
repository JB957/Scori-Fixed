package rabbitmq

import (
	"context"
	"encoding/json"
	"os"
	"runtime"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/scorify/scorify/pkg/config"
	"github.com/scorify/scorify/pkg/structs"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/process"
	"github.com/sirupsen/logrus"
)

const (
	HeartbeatQueue = "heartbeat_queue"
	HeartbeatVhost = "heartbeat_vhost"
)

var (
	// Permissions for minions in heartbeat vhosts
	HeartbeatConfigurePermissions   = regex(HeartbeatQueue)
	HeartbeatMinionWritePermissions = regex_amq_default(HeartbeatQueue)
	HeartbeatMinionReadPermissions  = regex("")
)

func heartbeatQueue(conn *amqp.Connection) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	q, err := ch.QueueDeclare(
		HeartbeatQueue,
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		_ = ch.Close()
		return nil, amqp.Queue{}, err
	}
	return ch, q, nil
}

type heartbeatListener struct {
	ch   *amqp.Channel
	msgs <-chan amqp.Delivery
}

func (r *RabbitMQConnections) HeartbeatListener(ctx context.Context) (*heartbeatListener, error) {
	ch, q, err := heartbeatQueue(r.Heartbeat)
	if err != nil {
		return nil, err
	}

	msgs, err := ch.ConsumeWithContext(
		ctx,
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = ch.Close()
		return nil, err
	}

	return &heartbeatListener{
		ch:   ch,
		msgs: msgs,
	}, nil
}

func (l *heartbeatListener) Close() error {
	return l.ch.Close()
}

func (l *heartbeatListener) Consume(ctx context.Context) (*structs.Heartbeat, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case msg, ok := <-l.msgs:
		if !ok {
			return nil, context.Canceled
		}
		var heartbeat structs.Heartbeat
		err := json.Unmarshal(msg.Body, &heartbeat)
		if err != nil {
			return nil, err
		}

		return &heartbeat, nil
	}
}

type heartbeatClient struct {
	ch         *amqp.Channel
	q          amqp.Queue
	lastCPU    float64
	lastSample time.Time
}

func (r *RabbitMQConnections) HeartbeatClient() (*heartbeatClient, error) {
	ch, q, err := heartbeatQueue(r.Heartbeat)
	if err != nil {
		return nil, err
	}

	return &heartbeatClient{
		ch: ch,
		q:  q,
	}, nil
}

func (c *heartbeatClient) Close() error {
	return c.ch.Close()
}

func (c *heartbeatClient) SendHeartbeat(ctx context.Context) error {
	start := time.Now()

	proc, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		return err
	}
	memoryInfo, err := proc.MemoryInfoWithContext(ctx)
	if err != nil {
		return err
	}
	memoryStats, err := mem.VirtualMemoryWithContext(ctx)
	if err != nil {
		return err
	}
	cpuTimes, err := proc.TimesWithContext(ctx)
	if err != nil {
		return err
	}
	now := time.Now()
	cpuUsage := 0.0
	if !c.lastSample.IsZero() {
		elapsed := now.Sub(c.lastSample).Seconds()
		if elapsed > 0 {
			cpuUsage = max(0, min(100, 100*(cpuTimes.Total()-c.lastCPU)/elapsed/float64(runtime.NumCPU())))
		}
	}
	c.lastCPU = cpuTimes.Total()
	c.lastSample = now

	metrics := structs.Heartbeat{
		MinionID:    config.Minion.ID,
		MemoryUsage: int64(memoryInfo.RSS),
		MemoryTotal: int64(memoryStats.Total),
		CPUUsage:    cpuUsage,
		Goroutines:  int64(runtime.NumGoroutine()),
	}

	out, err := json.Marshal(metrics)
	if err != nil {
		return err
	}

	err = c.ch.PublishWithContext(
		ctx,
		"",
		c.q.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        out,
		},
	)
	if err != nil {
		return err
	}

	logrus.WithField("time", now).Infof("Heartbeat sent to server in %s", now.Sub(start))

	return nil
}
