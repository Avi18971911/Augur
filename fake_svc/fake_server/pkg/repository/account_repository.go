package repository

import (
	"context"
	"fake_svc/fake_server/pkg/service/model"
)

type AccountRepository interface {
	GetAccountDetailsFromUsername(username string, ctx context.Context) (*model.AccountDetailsOutput, error)
}
