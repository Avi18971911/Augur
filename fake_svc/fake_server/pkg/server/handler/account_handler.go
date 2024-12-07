package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fake_svc/fake_server/pkg/service"
	"fake_svc/fake_server/pkg/service/model"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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
func AccountLoginHandler(
	s service.AccountService,
	ctx context.Context,
	tracer trace.Tracer,
	logger *logrus.Logger,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logger.Infof("Login request received with URL %s and method %s", r.URL.Path, r.Method)
		ctx, span := tracer.Start(ctx, "AccountLoginHandler")
		defer span.End()
		span.SetAttributes(
			attribute.String("http.url", r.URL.Path),
			attribute.String("username", r.Header.Get("username")),
			attribute.String("password", r.Header.Get("password")),
		)

		var req AccountLoginRequestDTO
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			logger.Errorf("Error encountered when decoding request body %v", err)
			HttpError(w, "Invalid request payload", http.StatusBadRequest, logger)
			return
		}

		defer func(Body io.ReadCloser) {
			err := Body.Close()
			if err != nil {
				logger.Errorf("Error encountered when closing request body %v", err)
			}
		}(r.Body)

		accountDetails, err := s.Login(req.Username, req.Password, ctx)
		if err != nil {
			if errors.Is(err, model.ErrInvalidCredentials) {
				logger.Errorf(
					"Invalid credentials for Login request with username: %s and password: %s",
					req.Username,
					req.Password,
				)
				HttpError(w, "Invalid credentials", http.StatusUnauthorized, logger)
				return
			}
			logger.Errorf("Error encountered during login %v", err)
			HttpError(w, "Error encountered during login", http.StatusInternalServerError, logger)
			return
		}

		jsonAccountDetails := accountDetailsToDTO(accountDetails)
		err = json.NewEncoder(w).Encode(jsonAccountDetails)
		if err != nil {
			logger.Errorf("Error encountered during JSON Encoding of Response %v", err)
			HttpError(
				w,
				"Error encountered during JSON Encoding of Response",
				http.StatusInternalServerError,
				logger,
			)
			return
		}
		logger.Infof("Login request successful with username: %s and password: %s", req.Username, req.Password)
	}
}
