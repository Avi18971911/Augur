package service

import (
	"errors"
	"github.com/shopspring/decimal"
	"time"
)

type AccountDetailsOutput struct {
	Id                string
	Username          string
	Password          string
	Person            Person
	BankAccounts      []BankAccount
	KnownBankAccounts []KnownBankAccount
	CreatedAt         time.Time
}

type BankAccountType string

const (
	Savings    BankAccountType = "savings"
	Checking   BankAccountType = "checking"
	Investment BankAccountType = "investment"
)

type Person struct {
	FirstName string
	LastName  string
}

type BankAccount struct {
	Id               string
	AccountNumber    string
	AccountType      BankAccountType
	PendingBalance   decimal.Decimal
	AvailableBalance decimal.Decimal
}

type KnownBankAccount struct {
	Id            string
	AccountNumber string
	AccountHolder string
	AccountType   BankAccountType
}

var (
	ErrNoMatchingUsername = errors.New("no matching username found for account")
	ErrInvalidCredentials = errors.New("invalid username or password")
)
