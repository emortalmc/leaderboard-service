package service

import (
	"context"
	"fmt"
	"github.com/emortalmc/proto-specs/gen/go/grpc/leaderboard"
	grpczap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"leaderboard-service/internal/config"
	"leaderboard-service/internal/kafka"
	"leaderboard-service/internal/redis"
	"leaderboard-service/internal/repository"
	"net"
	"sync"
)

func RunServices(ctx context.Context, logger *zap.SugaredLogger, wg *sync.WaitGroup, cfg *config.Config,
	repo repository.Repository, redis redis.Redis, notifier kafka.Notifier) {

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		logger.Fatalw("failed to listen", "err", err)
	}

	s := grpc.NewServer(grpc.ChainUnaryInterceptor(
		grpczap.UnaryServerInterceptor(logger.Desugar(), grpczap.WithLevels(func(code codes.Code) zapcore.Level {
			if code != codes.Internal && code != codes.Unavailable && code != codes.Unknown {
				return zapcore.DebugLevel
			} else {
				return zapcore.ErrorLevel
			}
		})),
	))

	if cfg.Development {
		reflection.Register(s)
	}

	svc := newLeaderboardService(repo, redis, notifier)
	leaderboard.RegisterLeaderboardServer(s, svc)

	logger.Infow("listening on port", "port", cfg.Port)

	go func() {
		if err := s.Serve(lis); err != nil {
			logger.Fatalw("failed to serve", "error", err)
			return
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		s.GracefulStop()
	}()
}
