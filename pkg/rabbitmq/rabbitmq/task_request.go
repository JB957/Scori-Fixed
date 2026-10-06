package rabbitmq

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/scorify/scorify/pkg/structs"
)

const (
	TaskRequestQueue = "task_request_queue"
	TaskRequestVhost = "task_request_vhost"
)

var (
	// Permissions for minions in task_request vhost
	TaskRequestConfigurePermissions   = regex(TaskRequestQueue)
	TaskRequestMinionWritePermissions = regex("")
	TaskRequestMinionReadPermissions  = regex(TaskRequestQueue)
)

func taskRequestQueue(conn *amqp.Connection) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	q, err := ch.QueueDeclare(
		TaskRequestQueue,
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

type taskRequestListener struct {
	ch   *amqp.Channel
	msgs <-chan amqp.Delivery
}

func (r *RabbitMQConnections) TaskRequestListener(ctx context.Context) (*taskRequestListener, error) {
	ch, q, err := taskRequestQueue(r.TaskRequest)
	if err != nil {
		return nil, err
	}
	if err := ch.Qos(32, 0, false); err != nil {
		_ = ch.Close()
		return nil, err
	}

	msgs, err := ch.ConsumeWithContext(
		ctx,
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = ch.Close()
		return nil, err
	}

	return &taskRequestListener{
		ch:   ch,
		msgs: msgs,
	}, nil
}

func (l *taskRequestListener) Close() error {
	return l.ch.Close()
}

func (l *taskRequestListener) Consume(ctx context.Context) (*structs.TaskRequest, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case msg, ok := <-l.msgs:
		if !ok {
			return nil, context.Canceled
		}
		var taskRequest structs.TaskRequest
		err := json.Unmarshal(msg.Body, &taskRequest)
		if err != nil {
			_ = msg.Ack(false)
			return nil, err
		}
		if err := msg.Ack(false); err != nil {
			return nil, err
		}

		return &taskRequest, nil
	}
}

type taskRequestClient struct {
	ch *amqp.Channel
	q  amqp.Queue
}

func (r *RabbitMQConnections) TaskRequestClient() (*taskRequestClient, error) {
	ch, q, err := taskRequestQueue(r.TaskRequest)
	if err != nil {
		return nil, err
	}

	return &taskRequestClient{
		ch: ch,
		q:  q,
	}, nil
}

func (c *taskRequestClient) Close() error {
	return c.ch.Close()
}

func (c *taskRequestClient) Publish(ctx context.Context, taskRequest *structs.TaskRequest) error {
	out, err := json.Marshal(taskRequest)
	if err != nil {
		return err
	}

	return c.ch.PublishWithContext(
		ctx,
		"",
		TaskRequestQueue,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        out,
		},
	)
}
