package service

import (
	"context"
	"errors"
	"fake_svc/fake_server/pkg/repository"
	"fake_svc/fake_server/pkg/transactional"
	"fmt"
	"log"
	"time"
)

const addTimeout = 5 * time.Second

type AccountServiceImpl struct {
	ar   repository.AccountRepository
	tran transactional.Transactional
}

func CreateNewAccountServiceImpl(
	ar repository.AccountRepository,
	tran transactional.Transactional,
) *AccountServiceImpl {
	return &AccountServiceImpl{
		ar:   ar,
		tran: tran,
	}
}

func (a *AccountServiceImpl) Login(
	username string,
	password string,
	ctx context.Context,
) (*AccountDetailsOutput, error) {
	getCtx, cancel := context.WithTimeout(ctx, addTimeout)
	defer cancel()

	txnCtx, err := a.tran.BeginTransaction(getCtx, transactional.IsolationLow, transactional.DurabilityLow)
	if err != nil {
		log.Printf("Error encountered when starting Login database transaction for "+
			"Username %s: ", username)
		return nil, fmt.Errorf("unable to begin transaction with error: %w", err)
	}

	defer func() {
		if rollErr := a.tran.Rollback(txnCtx); rollErr != nil {
			log.Printf("Error rolling back transaction: %v", rollErr)
		}
	}()

	accountDetails, err := a.ar.GetAccountDetailsFromUsername(username, getCtx)
	if err != nil {
		log.Printf("Unable to login with error: %v", err)
		if errors.Is(err, ErrNoMatchingUsername) {
			return nil, ErrInvalidCredentials
		}
		return nil, fmt.Errorf("unable to login with error: %w", err)
	}
	exists := accountDetails.Password == password
	if !exists {
		log.Printf("Login failed for Username %s", username)
		return nil, ErrInvalidCredentials
	}
	return accountDetails, nil
}
