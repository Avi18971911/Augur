package repository

import (
	"context"
	"errors"
	"fake_svc/fake_server/pkg/service"
	"fmt"
)

type AccountRepositoryMongodb struct {
}

func CreateNewAccountRepositoryMongodb() *AccountRepositoryMongodb {
	ar := AccountRepositoryMongodb{}
	return &ar
}

func (ar *AccountRepositoryMongodb) GetAccountDetailsFromUsername(
	username string,
	ctx context.Context,
) (*service.AccountDetailsOutput, error) {
	var accountDetails mongodb.MongoAccountOutput
	filter := bson.M{"username": username}
	err := ar.col.FindOne(ctx, filter).Decode(&accountDetails)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, model.ErrNoMatchingUsername
		}
		return nil, fmt.Errorf("error when finding account by username: %s", err.Error())
	}
	return fromMongoAccountDetails(&accountDetails)
}
