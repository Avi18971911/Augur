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
	BeginTransaction(ctx context.Context, isolationLevel int, durabilityLevel int) (TransactionContext, func(), error)
	Commit(txnCtx TransactionContext, cancelFunc func()) error
	Rollback(txnCtx TransactionContext, cancelFunc func()) error
}

type TransactionContext interface {
	context.Context
}
