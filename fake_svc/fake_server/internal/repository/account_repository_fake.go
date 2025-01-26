package repository

import (
	"context"
	"github.com/Avi18971911/Augur/fake_svc/fake_server/internal/fake_server_tracer"
	"github.com/Avi18971911/Augur/fake_svc/fake_server/internal/service/model"
	"github.com/shopspring/decimal"
	"github.com/sirupsen/logrus"
	"time"
)

type FakeAccountRepository struct {
	logger *logrus.Logger
}

func CreateNewFakeAccountRepository(logger *logrus.Logger) *FakeAccountRepository {
	ar := FakeAccountRepository{
		logger,
	}
	return &ar
}

var bobAmount, _ = decimal.NewFromString("123.23")
var bobPendingAmount, _ = decimal.NewFromString("0.00")
var ollyAccountId1 = "ollyAccountId1"
var hildaAccountId1 = "hildaAccountId1"

var fakeResult = &model.AccountDetailsOutput{
	Username: "fake_username",
	Password: "fake_password",
	Person: model.Person{
		FirstName: "Bob",
		LastName:  "Barker",
	},
	BankAccounts: []model.BankAccount{
		{
			Id:               ollyAccountId1,
			AccountNumber:    "123-12345-1",
			AccountType:      "savings",
			AvailableBalance: bobAmount,
			PendingBalance:   bobPendingAmount,
		},
	},
	KnownBankAccounts: []model.KnownBankAccount{
		{
			Id:            ollyAccountId1,
			AccountNumber: "123-12345-0",
			AccountHolder: "Olly OxenFree",
			AccountType:   "checking",
		},
		{
			Id:            hildaAccountId1,
			AccountNumber: "123-12345-2",
			AccountHolder: "Hilda Hill",
			AccountType:   "savings",
		},
	},
}

func (ar *FakeAccountRepository) GetAccountDetailsFromUsername(
	ctx context.Context,
	username string,
) (*model.AccountDetailsOutput, error) {
	ar.logger.Infof("Searching for the username in the DB: %s", username)

	tracer, err := fake_server_tracer.GetTracerFromContext(ctx)
	if err != nil {
		ar.logger.Errorf("Unable to get tracer from context during GetAccountDetailsFromUsername %v", err)
		return nil, err
	}
	ctx, span := tracer.Start(ctx, "AccountRepositoryGetAccountDetailsFromUsername")
	defer span.End()

	time.Sleep(1 * time.Second)
	ar.logger.Infof("Found the username in the DB: %s", username)
	return fakeResult, nil
}
