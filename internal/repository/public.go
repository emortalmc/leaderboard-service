package repository

import (
	"context"
	pb "github.com/emortalmc/proto-specs/gen/go/model/leaderboard"
	"google.golang.org/protobuf/types/known/anypb"
	"leaderboard-service/internal/repository/model"
)

type Repository interface {
	GetLeaderboard(ctx context.Context, id string) (*model.Leaderboard, error)
	CreateLeaderboard(ctx context.Context, id string, sortOrder pb.SortOrder) (*model.Leaderboard, error)
	DeleteLeaderboard(ctx context.Context, id string) error

	GetEntries(ctx context.Context, leaderboardId string, ids []string) (map[string]*model.LeaderboardEntry, error)

	CreateEntry(ctx context.Context, leaderboardId string, entryId string, score float64, data map[string]*anypb.Any) error
	DeleteEntry(ctx context.Context, leaderboardId string, entryId string) error

	UpdateScore(ctx context.Context, leaderboardId string, entryId string, score float64) error
}
