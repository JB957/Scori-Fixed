package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/scorify/scorify/pkg/static"
	"github.com/scorify/scorify/pkg/structs"
	"github.com/sirupsen/logrus"
)

const (
	// minion_heartbeat_key_prefix is the prefix for minion heartbeats
	minion_heartbeat_key_prefix = "minion_heartbeat"
)

func GetMinionHeartbeat(ctx context.Context, minionID uuid.UUID, redisClient *redis.Client) *structs.Heartbeat {
	data, err := redisClient.Get(ctx, fmt.Sprintf("%s:%s", minion_heartbeat_key_prefix, minionID.String())).Result()
	if err != nil {
		return nil
	}

	var minionHeartbeat structs.Heartbeat

	err = json.Unmarshal([]byte(data), &minionHeartbeat)
	if err != nil {
		return nil
	}

	return &minionHeartbeat
}

func SetMinionHeartbeat(ctx context.Context, minionID uuid.UUID, redisClient *redis.Client, metrics *structs.Heartbeat) error {
	data, err := json.Marshal(metrics)
	if err != nil {
		return err
	}

	return redisClient.Set(ctx, fmt.Sprintf("%s:%s", minion_heartbeat_key_prefix, minionID.String()), data, 30*time.Second).Err()
}

func GetMinionHeartbeatGroup(ctx context.Context, redisClient *redis.Client) ([]*structs.Heartbeat, error) {
	var keys []string
	iter := redisClient.Scan(ctx, 0, fmt.Sprintf("%s:*", minion_heartbeat_key_prefix), 100).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}

	data, err := redisClient.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	return static.MapSlice(data, func(_ int, value interface{}) *structs.Heartbeat {
		data, ok := value.(string)
		if !ok {
			logrus.WithField("value", value).Error("failed to convert value to string")
			return nil
		}

		var minionHeartbeat structs.Heartbeat
		err := json.Unmarshal([]byte(data), &minionHeartbeat)
		if err != nil {
			logrus.WithError(err).Error("failed to unmarshal data")
			return nil
		}

		return &minionHeartbeat
	}), nil
}
