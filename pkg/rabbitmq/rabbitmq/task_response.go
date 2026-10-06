package rabbitmq

import (
	"context"
	"encoding/json"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/scorify/scorify/pkg/structs"
)

const (
	TaskResponseQueue = "task_response_queue"
	TaskResponseVhost = "task_response_vhost"
)

var (
	// Permissions for minions in task_response vhost
	TaskResponseConfigurePermissions   = regex(TaskResponseQueue)
	TaskResponseMinionWritePermissions = regex_amq_default(TaskResponseQueue)
	TaskResponseMinionReadPermissions  = regex("")
)

func taskResponseQueue(conn *amqp.Connection) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	q, err := ch.QueueDeclare(
		TaskResponseQueue,
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

type taskResponseListener struct {
	ch   *amqp.Channel
	msgs <-chan amqp.Delivery
}

func (r *RabbitMQConnections) TaskResponseListener(ctx context.Context) (*taskResponseListener, error) {
	ch, q, err := taskResponseQueue(r.TaskResponse)
	if err != nil {
		return nil, err
	}
	if err := ch.Qos(64, 0, false); err != nil {
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

	return &taskResponseListener{
		ch:   ch,
		msgs: msgs,
	}, nil
}

func (l *taskResponseListener) Close() error {
	return l.ch.Close()
}

func (l *taskResponseListener) Consume(ctx context.Context) (*structs.TaskResponse, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case msg, ok := <-l.msgs:
		if !ok {
			return nil, context.Canceled
		}
		var taskResponse structs.TaskResponse
		err := json.Unmarshal(msg.Body, &taskResponse)
		if err != nil {
			_ = msg.Ack(false)
			return nil, err
		}
		if err := msg.Ack(false); err != nil {
			return nil, err
		}

		return &taskResponse, nil
	}
}

type taskResponseClient struct {
	ch   *amqp.Channel
	q    amqp.Queue
	lock sync.Mutex
}

func (r *RabbitMQConnections) TaskResponseClient() (*taskResponseClient, error) {
	ch, q, err := taskResponseQueue(r.TaskResponse)
	if err != nil {
		return nil, err
	}

	return &taskResponseClient{
		ch: ch,
		q:  q,
	}, nil
}

func (c *taskResponseClient) Close() error {
	return c.ch.Close()
}

func (c *taskResponseClient) SubmitTaskResponse(ctx context.Context, taskResponse *structs.TaskResponse) error {
	out, err := json.Marshal(taskResponse)
	if err != nil {
		return err
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	return c.ch.PublishWithContext(
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
}
