package router

import (
	"context"
	"fake_svc/fake_server/pkg/server/handler"
	"fake_svc/fake_server/pkg/service"
	"github.com/sirupsen/logrus"
	"net/http"
)
import "github.com/gorilla/mux"

func CreateRouter(
	accountService service.AccountService,
	ctx context.Context,
	logger *logrus.Logger,
) http.Handler {
	r := mux.NewRouter()
	r.Handle(
		"/accounts/login", handler.AccountLoginHandler(
			accountService,
			ctx,
			logger,
		),
	).Methods("POST")
	return r
}
