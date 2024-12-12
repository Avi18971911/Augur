package model

import "context"

type CountProcessorOutput struct {
	ctx              context.Context
	modifiedClusters []string
}
