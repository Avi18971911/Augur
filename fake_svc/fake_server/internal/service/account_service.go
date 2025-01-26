package service

import (
	"context"
	"github.com/Avi18971911/Augur/fake_svc/fake_server/internal/service/model"
)

type AccountService interface {
	Login(username string, password string, ctx context.Context) (*model.AccountDetailsOutput, error)
}
