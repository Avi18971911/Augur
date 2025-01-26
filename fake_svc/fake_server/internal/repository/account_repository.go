package repository

import (
	"context"
	"github.com/Avi18971911/Augur/fake_svc/fake_server/internal/service/model"
)

type AccountRepository interface {
	GetAccountDetailsFromUsername(ctx context.Context, username string) (*model.AccountDetailsOutput, error)
}
