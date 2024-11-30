package transactional

import (
	"context"
	"go.uber.org/zap"
	"time"
)

const timeout = 5 * time.Minute

type FakeTransactional struct {
	logger *zap.Logger
}

func NewFakeTransactional(logger *zap.Logger) *FakeTransactional {
	return &FakeTransactional{
		logger: logger,
	}
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
