package handler

import (
	"encoding/json"
	"go.uber.org/zap"
	"net/http"
)

type ErrorMessage struct {
	Message string `json:"message"`
}

func HttpError(w http.ResponseWriter, message string, statusCode int, logger *zap.Logger) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	err := json.NewEncoder(w).Encode(ErrorMessage{Message: message})
	if err != nil {
		logger.Error("Failed to encode error message", zap.Error(err))
	}
}
