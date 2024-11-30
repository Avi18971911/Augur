package transactional

import "context"

const (
	IsolationLow int = iota
	IsolationMedium
	IsolationHigh
)

const (
	DurabilityLow int = iota
	DurabilityHigh
)

type Transactional interface {
	BeginTransaction(ctx context.Context, isolationLevel int, durabilityLevel int) (TransactionContext, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type TransactionContext interface {
	context.Context
}
