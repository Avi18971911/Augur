package service

import (
	"context"
	"errors"
	"fake_svc/fake_server/pkg/repository"
	"fake_svc/fake_server/pkg/service/model"
	"fake_svc/fake_server/pkg/transactional"
	"fmt"
	"go.uber.org/zap"
	"time"
)

const addTimeout = 5 * time.Second

type AccountServiceImpl struct {
	ar     repository.AccountRepository
	tran   transactional.Transactional
	logger *zap.Logger
}

func CreateNewAccountServiceImpl(
	ar repository.AccountRepository,
	tran transactional.Transactional,
	logger *zap.Logger,
) *AccountServiceImpl {
	return &AccountServiceImpl{
		ar:     ar,
		tran:   tran,
		logger: logger,
	}
}

func (a *AccountServiceImpl) Login(
	username string,
	password string,
	ctx context.Context,
) (*model.AccountDetailsOutput, error) {
	a.logger.Info("Login request received", zap.String("username", username))
	getCtx, cancel := context.WithTimeout(ctx, addTimeout)
	defer cancel()

	txnCtx, err := a.tran.BeginTransaction(getCtx, transactional.IsolationLow, transactional.DurabilityLow)
	if err != nil {
		a.logger.Error("Unable to begin transaction", zap.Error(err))
		return nil, fmt.Errorf("unable to begin transaction with error: %w", err)
	}

	defer func() {
		if rollErr := a.tran.Rollback(txnCtx); rollErr != nil {
			a.logger.Error("Unable to rollback transaction", zap.Error(rollErr))
		}
	}()

	accountDetails, err := a.ar.GetAccountDetailsFromUsername(username, getCtx)
	if err != nil {
		a.logger.Error("Unable to get account details", zap.Error(err))
		if errors.Is(err, model.ErrNoMatchingUsername) {
			return nil, model.ErrInvalidCredentials
		}
		return nil, fmt.Errorf("unable to login with error: %w", err)
	}
	exists := accountDetails.Password == password
	if !exists {
		a.logger.Error("Invalid credentials")
		return nil, model.ErrInvalidCredentials
	}
	a.logger.Info(
		"Login successful with username and password",
		zap.String("username", username),
		zap.String("password", password),
	)
	return accountDetails, nil
}
