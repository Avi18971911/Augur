package router

import (
	"context"
	"github.com/Avi18971911/Augur/fake_svc/fake_server/internal/server/handler"
	"github.com/Avi18971911/Augur/fake_svc/fake_server/internal/service"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"
	"net/http"
)
import "github.com/gorilla/mux"

func CreateRouter(
	accountService service.AccountService,
	ctx context.Context,
	tracer trace.Tracer,
	logger *logrus.Logger,
) http.Handler {
	r := mux.NewRouter()
	r.Handle(
		"/accounts/login", handler.AccountLoginHandler(
			accountService,
			ctx,
			tracer,
			logger,
		),
	).Methods("POST")
	return r
}
