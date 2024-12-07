package service

import (
	"context"
	"errors"
	"fake_svc/fake_server/pkg/repository"
	"fake_svc/fake_server/pkg/service/model"
	"fake_svc/fake_server/pkg/transactional"
	"fmt"
	"github.com/sirupsen/logrus"
	"time"
)

const addTimeout = 5 * time.Second

type AccountServiceImpl struct {
	ar     repository.AccountRepository
	tran   transactional.Transactional
	logger *logrus.Logger
}

func CreateNewAccountServiceImpl(
	ar repository.AccountRepository,
	tran transactional.Transactional,
	logger *logrus.Logger,
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
	a.logger.Infof("Login request received with username: %s", "username")
	getCtx, cancel := context.WithTimeout(ctx, addTimeout)
	defer cancel()

	txnCtx, err := a.tran.BeginTransaction(getCtx, transactional.IsolationLow, transactional.DurabilityLow)
	if err != nil {
		a.logger.Errorf("Unable to begin transaction %v", err)
		return nil, fmt.Errorf("unable to begin transaction with error: %w", err)
	}

	defer func() {
		if rollErr := a.tran.Rollback(txnCtx); rollErr != nil {
			a.logger.Errorf("Unable to rollback transaction %v", rollErr)
		}
	}()

	accountDetails, err := a.ar.GetAccountDetailsFromUsername(username, getCtx)
	if err != nil {
		a.logger.Errorf("Unable to get account details %v", err)
		if errors.Is(err, model.ErrNoMatchingUsername) {
			return nil, model.ErrInvalidCredentials
		}
		return nil, fmt.Errorf("unable to login with error: %w", err)
	}
	exists := accountDetails.Password == password
	if !exists {
		a.logger.Errorf("Invalid credentials")
		return nil, model.ErrInvalidCredentials
	}
	a.logger.Infof("Login successful with username %s and password %s", username, password)
	return accountDetails, nil
}
