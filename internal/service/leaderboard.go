package service

import (
	"context"
	"errors"
	pb "github.com/emortalmc/proto-specs/gen/go/grpc/leaderboard"
	pbmodel "github.com/emortalmc/proto-specs/gen/go/model/leaderboard"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"leaderboard-service/internal/kafka"
	"leaderboard-service/internal/redis"
	"leaderboard-service/internal/repository"
	"log"
)

type leaderboardService struct {
	pb.LeaderboardServer

	repo     repository.Repository
	redis    redis.Redis
	notifier kafka.Notifier
}

func newLeaderboardService(repo repository.Repository, redis redis.Redis, notifier kafka.Notifier) pb.LeaderboardServer {
	return &leaderboardService{
		repo:     repo,
		redis:    redis,
		notifier: notifier,
	}
}

var (
	leaderboardNotFoundErr           = status.New(codes.NotFound, "leaderboard not found").Err()
	entryNotFoundErr                 = status.New(codes.NotFound, "leaderboard entry not found").Err()
	startRankGreaterThanEndPlaceErr  = status.New(codes.InvalidArgument, "start rank > end rank").Err()
	endRankGreaterThanLeaderboardErr = status.New(codes.InvalidArgument, "end rank > leaderboard size").Err()
)

func (s *leaderboardService) CreateLeaderboard(ctx context.Context, req *pb.CreateLeaderboardRequest) (*pb.CreateLeaderboardResponse, error) {
	leaderboard, err := s.repo.CreateLeaderboard(ctx, req.Id, req.SortOrder)
	if err != nil {
		return nil, err
	}

	pbLeaderboard := leaderboard.ToProto()
	s.notifier.LeaderboardCreated(ctx, pbLeaderboard)

	return &pb.CreateLeaderboardResponse{}, nil
}

func (s *leaderboardService) DeleteLeaderboard(ctx context.Context, req *pb.DeleteLeaderboardRequest) (*pb.DeleteLeaderboardResponse, error) {
	err := s.repo.DeleteLeaderboard(ctx, req.Id)
	if err != nil {
		return nil, convertErrIfNeeded(err)
	}

	s.notifier.LeaderboardDeleted(ctx, req.Id)

	return &pb.DeleteLeaderboardResponse{}, nil
}

func (s *leaderboardService) GetEntries(ctx context.Context, req *pb.GetEntriesRequest) (*pb.GetEntriesResponse, error) {
	if req.StartRank > req.EndRank {
		return nil, startRankGreaterThanEndPlaceErr
	}

	count, err := s.redis.GetEntryCount(ctx, req.LeaderboardId)
	if err != nil {
		return nil, err
	}

	if req.EndRank > count {
		return nil, endRankGreaterThanLeaderboardErr
	}

	leaderboard, err := s.repo.GetLeaderboard(ctx, req.LeaderboardId)
	if err != nil {
		return nil, convertErrIfNeeded(err)
	}

	ids, err := s.redis.GetEntriesInRange(ctx, req.LeaderboardId, leaderboard.SortOrder, req.StartRank, req.EndRank)
	if err != nil {
		return nil, err
	}

	entries, err := s.repo.GetEntries(ctx, req.LeaderboardId, ids)
	if err != nil {
		return nil, convertErrIfNeeded(err)
	}

	result := make([]*pbmodel.LeaderboardEntry, len(entries))
	rank := req.StartRank

	// The ids array is sorted. We return the results from Mongo in a map, so we can re-sort them here and assign them
	// the correct ranks.
	for i, id := range ids {
		result[i] = entries[id].ToProto(rank)
		rank++
	}

	log.Printf("result for get entries (svc): %+v", result)

	return &pb.GetEntriesResponse{Entries: result}, nil
}

func (s *leaderboardService) GetEntryCount(ctx context.Context, req *pb.GetEntryCountRequest) (*pb.GetEntryCountResponse, error) {
	count, err := s.redis.GetEntryCount(ctx, req.LeaderboardId)
	if err != nil {
		return nil, err
	}

	return &pb.GetEntryCountResponse{Count: count}, nil
}

func (s *leaderboardService) CreateEntry(ctx context.Context, req *pb.CreateEntryRequest) (*pb.CreateEntryResponse, error) {
	err := s.repo.CreateEntry(ctx, req.LeaderboardId, req.EntryId, req.Score, req.Data)
	if err != nil {
		return nil, err
	}

	err = s.redis.CreateOrUpdateEntry(ctx, req.LeaderboardId, req.EntryId, req.Score)
	if err != nil {
		return nil, err
	}

	rank, err := s.redis.GetEntryRank(ctx, req.LeaderboardId, req.EntryId)
	if err != nil {
		return nil, err
	}

	entry := &pbmodel.LeaderboardEntry{
		Id:    req.EntryId,
		Score: req.Score,
		Rank:  rank,
		Data:  req.Data,
	}
	s.notifier.EntryCreated(ctx, entry)

	return &pb.CreateEntryResponse{}, nil
}

func (s *leaderboardService) DeleteEntry(ctx context.Context, req *pb.DeleteEntryRequest) (*pb.DeleteEntryResponse, error) {
	err := s.repo.DeleteEntry(ctx, req.LeaderboardId, req.EntryId)
	if err != nil {
		return nil, convertErrIfNeeded(err)
	}

	err = s.redis.DeleteEntry(ctx, req.LeaderboardId, req.EntryId)
	if err != nil {
		return nil, err
	}

	s.notifier.EntryDeleted(ctx, req.EntryId)

	return &pb.DeleteEntryResponse{}, nil
}

func (s *leaderboardService) UpdateScore(ctx context.Context, req *pb.UpdateScoreRequest) (*pb.UpdateScoreResponse, error) {
	err := s.repo.UpdateScore(ctx, req.LeaderboardId, req.EntryId, req.Score)
	if err != nil {
		return nil, err
	}

	oldScore, err := s.redis.GetScore(ctx, req.LeaderboardId, req.EntryId)
	if err != nil {
		return nil, err
	}

	oldRank, err := s.redis.GetEntryRank(ctx, req.LeaderboardId, req.EntryId)
	if err != nil {
		return nil, err
	}

	err = s.redis.CreateOrUpdateEntry(ctx, req.LeaderboardId, req.EntryId, req.Score)
	if err != nil {
		return nil, err
	}

	newRank, err := s.redis.GetEntryRank(ctx, req.LeaderboardId, req.EntryId)
	if err != nil {
		return nil, err
	}

	s.notifier.ScoreUpdated(ctx, req.LeaderboardId, req.EntryId, oldScore, req.Score, oldRank, newRank)

	return &pb.UpdateScoreResponse{}, nil
}

func convertErrIfNeeded(err error) error {
	if errors.Is(err, repository.ErrLeaderboardNotFound) {
		return leaderboardNotFoundErr
	}

	if errors.Is(err, repository.ErrEntryNotFound) {
		return entryNotFoundErr
	}

	return err
}
