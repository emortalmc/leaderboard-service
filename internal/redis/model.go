package redis

type LeaderboardEntry struct {
	Id    string
	Score float64
	Rank  uint32
}
