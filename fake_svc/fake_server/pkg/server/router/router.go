package router

import (
	"context"
	"fake_svc/fake_server/pkg/server/handler"
	"fake_svc/fake_server/pkg/service"
	"net/http"
)
import "github.com/gorilla/mux"

func CreateRouter(
	accountService service.AccountService,
	ctx context.Context,
) http.Handler {
	r := mux.NewRouter()
	r.Handle("/accounts/login", handler.AccountLoginHandler(accountService, ctx)).Methods("POST")
	return r
}
