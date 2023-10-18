package app

import (
	"context"
	"go.uber.org/zap"
	"leaderboard-service/internal/config"
	"leaderboard-service/internal/kafka"
	lbRedis "leaderboard-service/internal/redis"
	"leaderboard-service/internal/repository"
	"leaderboard-service/internal/service"
	"os/signal"
	"sync"
	"syscall"
)

func Run(cfg *config.Config, logger *zap.SugaredLogger) {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	wg := &sync.WaitGroup{}

	delayedWg := &sync.WaitGroup{}
	delayedCtx, delayedCancel := context.WithCancel(ctx)

	repo, err := repository.NewMongoRepository(delayedCtx, logger, delayedWg, cfg.MongoDB)
	if err != nil {
		logger.Fatalw("failed to connect to mongo", err)
	}

	redis, err := lbRedis.NewRedis(delayedCtx, delayedWg, cfg.Redis)
	if err != nil {
		logger.Fatalw("failed to connect to redis", err)
	}

	notifier := kafka.NewNotifier(delayedCtx, delayedWg, cfg.Kafka, logger)
	if err != nil {
		logger.Fatalw("failed to create kafka notifier", err)
	}

	service.RunServices(ctx, logger, wg, cfg, repo, redis, notifier)

	wg.Wait()
	logger.Info("stopped services")

	logger.Info("shutting down repository, redis, and kafka")
	delayedCancel()
	delayedWg.Wait()
}
