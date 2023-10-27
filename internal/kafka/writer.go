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
	k.writeMessage(ctx, &lbmsg.LeaderboardCreatedMessage{Leaderboard: leaderboard})
}

func (k *kafkaNotifier) LeaderboardDeleted(ctx context.Context, leaderboardId string) {
	k.writeMessage(ctx, &lbmsg.LeaderboardDeletedMessage{LeaderboardId: leaderboardId})
}

func (k *kafkaNotifier) EntryCreated(ctx context.Context, entry *pb.LeaderboardEntry) {
	k.writeMessage(ctx, &lbmsg.EntryCreatedMessage{Entry: entry})
}

func (k *kafkaNotifier) EntryDeleted(ctx context.Context, entryId string) {
	k.writeMessage(ctx, &lbmsg.EntryDeletedMessage{EntryId: entryId})
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

	k.writeMessage(ctx, msg)
}

func (k *kafkaNotifier) writeMessage(ctx context.Context, msg proto.Message) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	bytes, err := proto.Marshal(msg)
	if err != nil {
		k.logger.Errorw("failed to marshal proto to bytes", "err", err)
	}

	protoName := msg.ProtoReflect().Descriptor().FullName()
	err = k.w.WriteMessages(ctx, kafka.Message{
		Headers: []kafka.Header{{Key: "X-Proto-Type", Value: []byte(protoName)}},
		Value:   bytes,
	})

	// The error is handled here as it's always the same basic error format for every message type
	if err != nil {
		k.logger.Errorw("failed to write message", "name", protoName, "err", err)
	}
}
