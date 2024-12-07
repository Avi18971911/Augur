package service

import (
	"context"
	"fake_svc/fake_server/pkg/service/model"
)

type AccountService interface {
	Login(username string, password string, ctx context.Context) (*model.AccountDetailsOutput, error)
}
