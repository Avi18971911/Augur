package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fake_svc/fake_server/pkg/service"
	"fake_svc/fake_server/pkg/service/model"
	"io"
	"log"
	"net/http"
)

// AccountLoginHandler creates a handler for logging in a user.
// @Summary Login
// @Description Logs in a user with the provided username and password.
// @Tags accounts
// @Accept json
// @Produce json
// @Param login body dto.AccountLoginRequestDTO true "Login payload"
// @Success 200 {object} dto.AccountDetailsResponseDTO "Successful login"
// @Failure 401 {object} utils.ErrorMessage "Invalid credentials"
// @Failure 500 {object} utils.ErrorMessage "Internal server error"
// @Router /accounts/login [post]
func AccountLoginHandler(s service.AccountService, ctx context.Context) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req AccountLoginRequestDTO
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			HttpError(w, "Invalid request payload", http.StatusBadRequest)
			return
		}

		defer func(Body io.ReadCloser) {
			err := Body.Close()
			if err != nil {
				log.Printf("Failed to close request body: %v", err)
			}
		}(r.Body)

		accountDetails, err := s.Login(req.Username, req.Password, ctx)
		if err != nil {
			if errors.Is(err, model.ErrInvalidCredentials) {
				HttpError(w, "Invalid credentials", http.StatusUnauthorized)
				return
			}
			HttpError(w, "Error encountered during login", http.StatusInternalServerError)
			return
		}

		jsonAccountDetails := accountDetailsToDTO(accountDetails)
		err = json.NewEncoder(w).Encode(jsonAccountDetails)
		if err != nil {
			HttpError(w, "Error encountered during JSON Encoding of Response",
				http.StatusInternalServerError)
			return
		}

	}
}
