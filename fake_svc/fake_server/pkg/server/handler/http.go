package handler

import (
	"encoding/json"
	"log"
	"net/http"
)

type ErrorMessage struct {
	Message string `json:"message"`
}

func HttpError(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	err := json.NewEncoder(w).Encode(ErrorMessage{Message: message})
	if err != nil {
		log.Printf("Failed to encode error message: %v", err)
	}
}
