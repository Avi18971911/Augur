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
	ft.logger.Info(
		"Beginning Transaction with concerns",
		zap.String("readConcern", toStringReadConcern(readConcern)),
		zap.String("writeConcern", toStringWriteConcern(writeConcern)),
	)
	txnCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return txnCtx, nil
}

func (ft *FakeTransactional) Commit(ctx context.Context) error {
	ft.logger.Info("Committing Transaction")
	return nil
}

func (ft *FakeTransactional) Rollback(ctx context.Context) error {
	ft.logger.Info("Rolling back Transaction")
	return nil
}

func toStringReadConcern(concern int) string {
	switch concern {
	case IsolationLow:
		return "IsolationLow"
	case IsolationMedium:
		return "IsolationMedium"
	case IsolationHigh:
		return "IsolationHigh"
	default:
		return "Unknown"
	}
}

func toStringWriteConcern(concern int) string {
	switch concern {
	case DurabilityLow:
		return "DurabilityLow"
	case DurabilityHigh:
		return "DurabilityHigh"
	default:
		return "Unknown"
	}
}
