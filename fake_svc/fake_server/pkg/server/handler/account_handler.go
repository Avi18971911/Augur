package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fake_svc/fake_server/pkg/service"
	"fake_svc/fake_server/pkg/service/model"
	"go.uber.org/zap"
	"io"
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
func AccountLoginHandler(s service.AccountService, ctx context.Context, logger *zap.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logger.Info("Login request received", zap.String("URL", r.URL.Path))
		var req AccountLoginRequestDTO
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			logger.Error("Error encountered when decoding request body", zap.Error(err))
			HttpError(w, "Invalid request payload", http.StatusBadRequest)
			return
		}

		defer func(Body io.ReadCloser) {
			err := Body.Close()
			if err != nil {
				logger.Error("Error encountered when closing request body", zap.Error(err))
			}
		}(r.Body)

		accountDetails, err := s.Login(req.Username, req.Password, ctx)
		if err != nil {
			if errors.Is(err, model.ErrInvalidCredentials) {
				logger.Error(
					"Invalid credentials for Login request",
					zap.String("username", req.Username),
					zap.String("password", req.Password),
				)
				HttpError(w, "Invalid credentials", http.StatusUnauthorized)
				return
			}
			logger.Error("Error encountered during login", zap.Error(err))
			HttpError(w, "Error encountered during login", http.StatusInternalServerError)
			return
		}

		jsonAccountDetails := accountDetailsToDTO(accountDetails)
		err = json.NewEncoder(w).Encode(jsonAccountDetails)
		if err != nil {
			logger.Error("Error encountered during JSON Encoding of Response", zap.Error(err))
			HttpError(w, "Error encountered during JSON Encoding of Response",
				http.StatusInternalServerError)
			return
		}
		logger.Info(
			"Login request successful",
			zap.String("username", req.Username),
			zap.String("password", req.Password),
		)
	}
}
