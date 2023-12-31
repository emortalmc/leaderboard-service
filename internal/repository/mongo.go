package repository

import (
	"context"
	"errors"
	pb "github.com/emortalmc/proto-specs/gen/go/model/leaderboard"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"
	"leaderboard-service/internal/config"
	"leaderboard-service/internal/repository/model"
	"sync"
	"time"
)

const (
	databaseName = "leaderboards"

	leaderboardCollectionName = "leaderboards"
	entryCollectionName       = "entries"
)

var (
	ErrLeaderboardNotFound = errors.New("leaderboard not found")
	ErrEntryNotFound       = errors.New("leaderboard entry not found")
)

type mongoRepository struct {
	database *mongo.Database

	leaderboardCollection *mongo.Collection
	entryCollection       *mongo.Collection
}

func NewMongoRepository(ctx context.Context, logger *zap.SugaredLogger, wg *sync.WaitGroup, cfg *config.MongoDBConfig) (Repository, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.URI))
	if err != nil {
		return nil, err
	}

	database := client.Database(databaseName)
	repo := &mongoRepository{
		database: database,

		leaderboardCollection: database.Collection(leaderboardCollectionName),
		entryCollection:       database.Collection(entryCollectionName),
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		if err := client.Disconnect(ctx); err != nil {
			logger.Errorw("failed to disconnect from mongo", "err", err)
		}
	}()

	return repo, nil
}

// hasLeaderboard returns ErrLeaderboardNotFound to improve client usage
func (m *mongoRepository) hasLeaderboard(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	count, err := m.leaderboardCollection.CountDocuments(ctx, bson.M{"_id": id})
	if err != nil {
		return err
	}

	if count == 0 {
		return ErrLeaderboardNotFound
	}

	return nil
}

func (m *mongoRepository) GetLeaderboard(ctx context.Context, id string) (*model.Leaderboard, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var leaderboard model.Leaderboard

	err := m.leaderboardCollection.FindOne(ctx, bson.M{"_id": id}).Decode(&leaderboard)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrLeaderboardNotFound
		}
		return nil, err
	}

	return &leaderboard, nil
}

func (m *mongoRepository) CreateLeaderboard(ctx context.Context, values *pb.Leaderboard) (*model.Leaderboard, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	id := values.Id
	leaderboard := &model.Leaderboard{
		Id:        id,
		SortOrder: values.SortOrder,
	}

	_, err := m.leaderboardCollection.InsertOne(ctx, leaderboard)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) || mongo.IsDuplicateKeyError(err) {
			// Don't care if there's already a leaderboard in there, just ignore the error.
			return leaderboard, nil
		}
		return nil, err
	}

	return leaderboard, nil
}

func (m *mongoRepository) DeleteLeaderboard(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := m.leaderboardCollection.DeleteOne(ctx, bson.M{"_id": id})
	return err
}

func (m *mongoRepository) GetEntries(ctx context.Context, leaderboardId string, entryIds []string) (map[string]*model.LeaderboardEntry, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := m.hasLeaderboard(ctx, leaderboardId); err != nil {
		return nil, err
	}

	cursor, err := m.entryCollection.Find(ctx, bson.M{"leaderboardId": leaderboardId, "_id": bson.M{"$in": entryIds}})
	if err != nil {
		return nil, err
	}

	var entries []*model.LeaderboardEntry
	err = cursor.All(ctx, &entries)
	if err != nil {
		return nil, err
	}

	result := make(map[string]*model.LeaderboardEntry, len(entries))
	for _, entry := range entries {
		result[entry.Id] = entry
	}

	return result, nil
}

func (m *mongoRepository) CreateEntry(ctx context.Context, leaderboardId string, entryId string, score float64, data map[string]*anypb.Any) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := m.hasLeaderboard(ctx, leaderboardId); err != nil {
		return err
	}

	entry := &model.LeaderboardEntry{
		LeaderboardId: leaderboardId,
		Id:            entryId,
		Score:         score,
		Data:          data,
	}

	_, err := m.entryCollection.InsertOne(ctx, entry)
	if errors.Is(err, mongo.ErrNoDocuments) || mongo.IsDuplicateKeyError(err) {
		// Don't care if there's already an entry in there, just ignore the error.
		return nil
	}
	return err
}

func (m *mongoRepository) DeleteEntry(ctx context.Context, leaderboardId string, entryId string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := m.hasLeaderboard(ctx, leaderboardId); err != nil {
		return err
	}

	_, err := m.entryCollection.DeleteOne(ctx, bson.M{"leaderboardId": leaderboardId, "_id": entryId})
	return err
}

func (m *mongoRepository) UpdateScore(ctx context.Context, leaderboardId string, entryId string, score float64) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := m.hasLeaderboard(ctx, leaderboardId); err != nil {
		return err
	}

	_, err := m.entryCollection.UpdateOne(ctx, bson.M{"leaderboardId": leaderboardId, "_id": entryId}, bson.M{"$set": bson.M{"score": score}})
	return err
}
