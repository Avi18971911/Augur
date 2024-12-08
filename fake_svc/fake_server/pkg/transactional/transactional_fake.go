package transactional

import (
	"context"
	"fake_svc/fake_server/pkg/fake_server_tracer"
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
) (TransactionContext, func(), error) {
	ft.logger.Infof(
		"Beginning Transaction with concerns readConcern: %s, writeConcern: %s",
		toStringReadConcern(readConcern),
		toStringWriteConcern(writeConcern),
	)
	tracer, err := fake_server_tracer.GetTracerFromContext(ctx)
	if err != nil {
		return nil, nil, err
	}

	txnCtx, cancel := context.WithTimeout(ctx, timeout)
	txnCtx, span := tracer.Start(txnCtx, "BeginTransaction")
	cancelFunc := func() {
		span.End()
		cancel()
	}
	return txnCtx, cancelFunc, nil
}

func (ft *FakeTransactional) Commit(txnCtx TransactionContext, cancelFunc func()) error {
	ft.logger.Infof("Committing Transaction")
	cancelFunc()
	return nil
}

func (ft *FakeTransactional) Rollback(txnCtx TransactionContext, cancelFunc func()) error {
	ft.logger.Infof("Rolling back Transaction")
	cancelFunc()
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
