package repository

import (
	"context"
	"fake_svc/fake_server/pkg/service/model"
)

type AccountRepository interface {
	GetAccountDetailsFromUsername(ctx context.Context, username string) (*model.AccountDetailsOutput, error)
}
