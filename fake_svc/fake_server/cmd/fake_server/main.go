package main

import (
	"context"
	"fake_svc/fake_server/pkg/repository"
	"fake_svc/fake_server/pkg/server/router"
	"fake_svc/fake_server/pkg/service"
	"fake_svc/fake_server/pkg/transactional"
	"github.com/sirupsen/logrus"
	"net/http"
	"os"
	"time"
)

func initLogger() *logrus.Logger {
	log := logrus.New()
	log.Level = logrus.DebugLevel
	log.Formatter = &logrus.JSONFormatter{
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "timestamp",
			logrus.FieldKeyLevel: "severity",
			logrus.FieldKeyMsg:   "message",
		},
		TimestampFormat: time.RFC3339Nano,
	}
	log.Out = os.Stdout
	return log
}

func main() {
	logger := initLogger()

	ar := repository.CreateNewFakeAccountRepository(logger)
	tra := transactional.NewFakeTransactional(logger)
	as := service.CreateNewAccountServiceImpl(ar, tra, logger)
	r := router.CreateRouter(as, context.Background(), logger)

	logger.Infof("Starting webserver")
	logger.Fatalf("Stopped Listening to Webserver! %v", http.ListenAndServe(":8080", r))
}
