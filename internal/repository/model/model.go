package model

import (
	pb "github.com/emortalmc/proto-specs/gen/go/model/leaderboard"
	"google.golang.org/protobuf/types/known/anypb"
)

type Leaderboard struct {
	Id        string       `bson:"_id"`
	SortOrder pb.SortOrder `bson:"sort_order"`
}

func (l *Leaderboard) ToProto() *pb.Leaderboard {
	return &pb.Leaderboard{
		Id:        l.Id,
		SortOrder: l.SortOrder,
	}
}

type LeaderboardEntry struct {
	LeaderboardId string `bson:"leaderboard_id"`

	Id    string  `bson:"_id"`
	Score float64 `bson:"score"`

	Data map[string]*anypb.Any `bson:"data"`
}

func (e *LeaderboardEntry) ToProto(rank uint32) *pb.LeaderboardEntry {
	return &pb.LeaderboardEntry{
		Id:    e.Id,
		Score: e.Score,
		Rank:  rank,
		Data:  e.Data,
	}
}
