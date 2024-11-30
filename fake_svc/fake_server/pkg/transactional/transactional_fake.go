package transactional

import (
	"context"
	"time"
)

const timeout = 5 * time.Minute

type FakeTransactional struct{}

func NewFakeTransactional() *FakeTransactional {
	return &FakeTransactional{}
}

func (ft *FakeTransactional) BeginTransaction(
	ctx context.Context,
	readConcern int,
	writeConcern int,
) (TransactionContext, error) {
	txnCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return txnCtx, nil
}

func (ft *FakeTransactional) Commit(ctx context.Context) error {
	return nil
}

func (ft *FakeTransactional) Rollback(ctx context.Context) error {
	return nil
}
