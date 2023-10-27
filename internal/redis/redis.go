package redis

import (
	"context"
	"fmt"
	pb "github.com/emortalmc/proto-specs/gen/go/model/leaderboard"
	"github.com/redis/rueidis"
	"leaderboard-service/internal/config"
	"sync"
	"time"
)

type Redis interface {
	// CreateOrUpdateEntry creates or updates an entry in the leaderboard.
	// Because of the way Redis sorted sets work, this will also create the leaderboard if it doesn't exist.
	CreateOrUpdateEntry(ctx context.Context, leaderboardId string, entryId string, score float64) error
	DeleteEntry(ctx context.Context, leaderboardId string, entryId string) error

	GetEntriesInRange(ctx context.Context, leaderboardId string, sortOrder pb.SortOrder, startRank uint32, endRank uint32) ([]string, error)
	GetEntryRank(ctx context.Context, leaderboardId string, entryId string) (uint32, error)
	GetEntryCount(ctx context.Context, leaderboardId string) (uint32, error)
	GetScore(ctx context.Context, leaderboardId string, entryId string) (float64, error)
}

type redisImpl struct {
	client rueidis.Client
}

func NewRedis(ctx context.Context, wg *sync.WaitGroup, cfg *config.RedisConfig) (Redis, error) {
	client, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)}})
	if err != nil {
		return nil, err
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		client.Close()
	}()

	return &redisImpl{
		client: client,
	}, nil
}

func (r *redisImpl) CreateOrUpdateEntry(ctx context.Context, leaderboardId string, entryId string, score float64) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	cmd := r.client.B().Zadd().Key(addPrefix(leaderboardId)).ScoreMember().ScoreMember(score, entryId).Build()

	err := r.client.Do(ctx, cmd).Error()
	return err
}

func (r *redisImpl) DeleteEntry(ctx context.Context, leaderboardId string, entryId string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	cmd := r.client.B().Zrem().Key(addPrefix(leaderboardId)).Member(entryId).Build()

	err := r.client.Do(ctx, cmd).Error()
	return err
}

func (r *redisImpl) GetEntriesInRange(ctx context.Context, leaderboardId string, sortOrder pb.SortOrder, startRank uint32, endRank uint32) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	cmdBuilder := r.client.B().Zrange().Key(addPrefix(leaderboardId)).Min(fmt.Sprint(startRank)).Max(fmt.Sprint(endRank))

	var cmd rueidis.Completed
	if sortOrder == pb.SortOrder_ASCENDING {
		cmd = cmdBuilder.Build()
	} else if sortOrder == pb.SortOrder_DESCENDING {
		cmd = cmdBuilder.Rev().Build()
	} else {
		return nil, fmt.Errorf("invalid sort order: %d", sortOrder)
	}

	res, err := r.client.Do(ctx, cmd).AsStrSlice()
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (r *redisImpl) GetEntryRank(ctx context.Context, leaderboardId string, entryId string) (uint32, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	cmd := r.client.B().Zrank().Key(addPrefix(leaderboardId)).Member(entryId).Build()

	res := r.client.Do(ctx, cmd)
	return convertResultToUint32(res)
}

func (r *redisImpl) GetEntryCount(ctx context.Context, leaderboardId string) (uint32, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	cmd := r.client.B().Zcard().Key(addPrefix(leaderboardId)).Build()

	res := r.client.Do(ctx, cmd)
	return convertResultToUint32(res)
}

func convertResultToUint32(result rueidis.RedisResult) (uint32, error) {
	msg, err := result.ToMessage()
	if err != nil {
		return 0, err
	}

	res, err := msg.AsUint64()
	if err != nil {
		return 0, err
	}

	return uint32(res), nil
}

func (r *redisImpl) GetScore(ctx context.Context, leaderboardId string, entryId string) (float64, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	cmd := r.client.B().Zscore().Key(addPrefix(leaderboardId)).Member(entryId).Build()

	res, err := r.client.Do(ctx, cmd).AsFloat64()
	if err != nil {
		return 0, err
	}

	return res, nil
}

func addPrefix(key string) string {
	return fmt.Sprintf("leaderboard:%s", key)
}
