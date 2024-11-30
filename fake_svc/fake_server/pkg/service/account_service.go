package service

import (
	"context"
)

type AccountService interface {
	Login(username string, password string, ctx context.Context) (*AccountDetailsOutput, error)
}
