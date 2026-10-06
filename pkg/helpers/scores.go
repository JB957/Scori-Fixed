package helpers

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/scorify/scorify/pkg/cache"
	"github.com/scorify/scorify/pkg/ent"
	"github.com/scorify/scorify/pkg/ent/status"
)

func RecomputeScores(tx *ent.Tx, redisClient *redis.Client, ctx context.Context) error {
	entRounds, err := tx.Round.Query().All(ctx)
	if err != nil {
		return fmt.Errorf("failed to get rounds: %v", err)
	}

	_, err = tx.ScoreCache.Delete().Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to delete old score caches: %v", err)
	}

	type ScoreCaches_t struct {
		UserID uuid.UUID `json:"user_id"`
		Score  int       `json:"sum"`
	}

	for _, entRound := range entRounds {
		scoreCaches := make([]ScoreCaches_t, 0)

		err = tx.Status.Query().
			Where(
				status.RoundID(entRound.ID),
			).
			GroupBy(
				status.FieldUserID,
			).
			Aggregate(
				ent.Sum(
					status.FieldPoints,
				),
			).
			Scan(ctx, &scoreCaches)
		if err != nil {
			return fmt.Errorf("failed to get scores: %v", err)
		}

		scoreCacheUpdates := make([]*ent.ScoreCacheCreate, 0, len(scoreCaches))
		for _, scoreCache := range scoreCaches {
			scoreCacheUpdates = append(
				scoreCacheUpdates,
				tx.ScoreCache.Create().
					SetUserID(scoreCache.UserID).
					SetRoundID(entRound.ID).
					SetPoints(scoreCache.Score),
			)
		}
		if len(scoreCacheUpdates) > 0 {
			if _, err = tx.ScoreCache.CreateBulk(scoreCacheUpdates...).Save(ctx); err != nil {
				return fmt.Errorf("failed to save score caches for round %d: %v", entRound.Number, err)
			}
		}
	}

	scoreboard, err := Scoreboard(ctx, tx.Client())
	if err != nil {
		return fmt.Errorf("failed to get scoreboard: %v", err)
	}

	_, err = cache.PublishScoreboardUpdate(ctx, redisClient, scoreboard)

	return err
}
