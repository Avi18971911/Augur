package transactional

import (
	"context"
	"errors"
)

type MongoTransactional struct{}

func NewMongoTransactional() *MongoTransactional {
	return &MongoTransactional{}
}

func (m *MongoTransactional) BeginTransaction(
	ctx context.Context,
	readConcern int,
	writeConcern int,
) (TransactionContext, error) {
	session, err := m.client.StartSession()
	if err != nil {
		return nil, err
	}
	m.session = session

	determinedReadConcern := determineReadConcern(readConcern)
	determinedWriteConcern := determineWriteConcern(writeConcern)
	txnOpts := options.Transaction().SetReadConcern(determinedReadConcern).SetWriteConcern(determinedWriteConcern)
	err = session.StartTransaction(txnOpts)
	if err != nil {
		return nil, err
	}

	txnCtx := mongo.NewSessionContext(ctx, session)
	return txnCtx, nil
}

func determineReadConcern(readConcern int) *readconcern.ReadConcern {
	switch readConcern {
	case IsolationLow:
		return readconcern.Local()
	case IsolationMedium:
		return readconcern.Majority()
	case IsolationHigh:
		return readconcern.Snapshot()
	default:
		return readconcern.Majority()
	}
}

func determineWriteConcern(writeConcern int) *writeconcern.WriteConcern {
	switch writeConcern {
	case DurabilityLow:
		return writeconcern.W1()
	case DurabilityHigh:
		return writeconcern.Majority()
	default:
		return writeconcern.Majority()
	}
}

func (m *MongoTransactional) Commit(ctx context.Context) error {
	if m.session != nil {
		err := m.session.CommitTransaction(ctx)
		m.session.EndSession(ctx)
		return err
	}
	return errors.New("no session found, please start a transaction before committing")
}

func (m *MongoTransactional) Rollback(ctx context.Context) error {
	if m.session != nil {
		err := m.session.AbortTransaction(ctx)
		m.session.EndSession(ctx)
		return err
	}
	return errors.New("no session found, please start a transaction before rolling back")
}
