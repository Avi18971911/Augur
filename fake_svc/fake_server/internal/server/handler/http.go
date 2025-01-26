package handler

import (
	"encoding/json"
	"github.com/sirupsen/logrus"
	"net/http"
)

type ErrorMessage struct {
	Message string `json:"message"`
}

func HttpError(w http.ResponseWriter, message string, statusCode int, logger *logrus.Logger) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	err := json.NewEncoder(w).Encode(ErrorMessage{Message: message})
	if err != nil {
		logger.Errorf("Failed to encode error message: %v", err)
	}
}
