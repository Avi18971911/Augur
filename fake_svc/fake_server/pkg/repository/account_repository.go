package repository

import (
	"context"
	"fake_svc/fake_server/pkg/service"
)

type AccountRepository interface {
	GetAccountDetailsFromUsername(username string, ctx context.Context) (*service.AccountDetailsOutput, error)
}
