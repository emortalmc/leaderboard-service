package kafka

import (
	"context"
	"fmt"
	lbmsg "github.com/emortalmc/proto-specs/gen/go/message/leaderboard"
	pb "github.com/emortalmc/proto-specs/gen/go/model/leaderboard"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"leaderboard-service/internal/config"
	"sync"
	"time"
)

const writeTopic = "leaderboard-service"

type Notifier interface {
	LeaderboardCreated(ctx context.Context, leaderboard *pb.Leaderboard)
	LeaderboardDeleted(ctx context.Context, leaderboardId string)
	EntryCreated(ctx context.Context, entry *pb.LeaderboardEntry)
	EntryDeleted(ctx context.Context, entryId string)
	ScoreUpdated(ctx context.Context, leaderboardId string, entryId string, oldScore float64, newScore float64, oldRank uint32, newRank uint32)
}

type kafkaNotifier struct {
	logger *zap.SugaredLogger
	w      *kafka.Writer
}

func NewNotifier(ctx context.Context, wg *sync.WaitGroup, cfg *config.KafkaConfig, logger *zap.SugaredLogger) Notifier {
	w := &kafka.Writer{
		Addr:         kafka.TCP(fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)),
		Topic:        writeTopic,
		Balancer:     &kafka.LeastBytes{},
		Async:        true,
		BatchTimeout: 100 * time.Millisecond,
		ErrorLogger:  kafka.LoggerFunc(logger.Errorw),
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		if err := w.Close(); err != nil {
			logger.Errorw("failed to close kafka writer", "err", err)
		}
	}()

	return &kafkaNotifier{
		logger: logger,
		w:      w,
	}
}

func (k *kafkaNotifier) LeaderboardCreated(ctx context.Context, leaderboard *pb.Leaderboard) {
	msg := &lbmsg.LeaderboardCreatedMessage{Leaderboard: leaderboard}

	if err := k.writeMessage(ctx, msg); err != nil {
		k.logger.Errorw("failed to write leaderboard created message", "err", err)
	}
}

func (k *kafkaNotifier) LeaderboardDeleted(ctx context.Context, leaderboardId string) {
	msg := &lbmsg.LeaderboardDeletedMessage{LeaderboardId: leaderboardId}

	if err := k.writeMessage(ctx, msg); err != nil {
		k.logger.Errorw("failed to write leaderboard deleted message", "err", err)
	}
}

func (k *kafkaNotifier) EntryCreated(ctx context.Context, entry *pb.LeaderboardEntry) {
	msg := &lbmsg.EntryCreatedMessage{Entry: entry}

	if err := k.writeMessage(ctx, msg); err != nil {
		k.logger.Errorw("failed to write entry created message", "err", err)
	}
}

func (k *kafkaNotifier) EntryDeleted(ctx context.Context, entryId string) {
	msg := &lbmsg.EntryDeletedMessage{EntryId: entryId}

	if err := k.writeMessage(ctx, msg); err != nil {
		k.logger.Errorw("failed to write entry deleted message", "err", err)
	}
}

func (k *kafkaNotifier) ScoreUpdated(ctx context.Context, leaderboardId string, entryId string, oldScore float64, newScore float64, oldRank uint32, newRank uint32) {
	msg := &lbmsg.ScoreUpdatedMessage{
		LeaderboardId: leaderboardId,
		EntryId:       entryId,
		OldScore:      oldScore,
		NewScore:      newScore,
		OldRank:       oldRank,
		NewRank:       newRank,
	}

	if err := k.writeMessage(ctx, msg); err != nil {
		k.logger.Errorw("failed to write score updated message", "err", err)
	}
}

func (k *kafkaNotifier) writeMessage(ctx context.Context, msg proto.Message) error {
	bytes, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal proto to bytes: %s", err)
	}

	return k.w.WriteMessages(ctx, kafka.Message{
		Headers: []kafka.Header{{Key: "X-Proto-Type", Value: []byte(msg.ProtoReflect().Descriptor().FullName())}},
		Value:   bytes,
	})
}
