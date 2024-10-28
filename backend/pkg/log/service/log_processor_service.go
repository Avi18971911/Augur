package service

import "github.com/Avi18971911/Augur/pkg/log/model"

type LogProcessorRepository interface {
	// FindLogsOfServiceWithPhrase FindLogsOfService Note this function should only ever return a subset of the total logs
	FindLogsOfServiceWithPhrase(service string, phrase string) ([]model.LogEntry, error)
}

type LogProcessorService struct {
}

func NewLogProcessorService() *LogProcessorService {
	return &LogProcessorService{}
}
