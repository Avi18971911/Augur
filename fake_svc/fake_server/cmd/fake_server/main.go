package main

import (
	"context"
	"fake_svc/fake_server/pkg/repository"
	"fake_svc/fake_server/pkg/server/router"
	"fake_svc/fake_server/pkg/service"
	"fake_svc/fake_server/pkg/transactional"
	"log"
	"net/http"
)

func main() {
	ar := repository.CreateNewFakeAccountRepository()

	tra := transactional.NewFakeTransactional()

	as := service.CreateNewAccountServiceImpl(ar, tra)
	r := router.CreateRouter(as, context.Background())
	log.Printf("Starting webserver")
	log.Fatal(http.ListenAndServe(":8080", r))
}
