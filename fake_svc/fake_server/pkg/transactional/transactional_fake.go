package transactional

import (
	"context"
	"github.com/sirupsen/logrus"
	"time"
)

const timeout = 5 * time.Minute

type FakeTransactional struct {
	logger *logrus.Logger
}

func NewFakeTransactional(logger *logrus.Logger) *FakeTransactional {
	return &FakeTransactional{
		logger: logger,
	}
}

func (ft *FakeTransactional) BeginTransaction(
	ctx context.Context,
	readConcern int,
	writeConcern int,
) (TransactionContext, error) {
	ft.logger.Infof(
		"Beginning Transaction with concerns readConcern: %s, writeConcern: %s",
		toStringReadConcern(readConcern),
		toStringWriteConcern(writeConcern),
	)
	txnCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return txnCtx, nil
}

func (ft *FakeTransactional) Commit(ctx context.Context) error {
	ft.logger.Infof("Committing Transaction")
	return nil
}

func (ft *FakeTransactional) Rollback(ctx context.Context) error {
	ft.logger.Infof("Rolling back Transaction")
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
