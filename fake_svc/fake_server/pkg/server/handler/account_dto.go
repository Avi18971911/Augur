package handler

import (
	"fake_svc/fake_server/pkg/service"
	"time"
)

// AccountLoginRequestDTO represents the login credentials for an account
// @swagger:model AccountLoginRequestDTO
type AccountLoginRequestDTO struct {
	// The username for the login
	Username string `json:"username" validate:"required"`
	// The password for the login
	Password string `json:"password" validate:"required"`
}

// AccountDetailsResponseDTO represents the confidential details of an account belonging to a customer
// @swagger:model AccountDetailsResponseDTO
type AccountDetailsResponseDTO struct {
	// The unique identifier of the account
	Id string `json:"id" validate:"required"`
	// The username associated with the account
	Username string `json:"username" validate:"required"`
	// The account holder associated with the account
	Person PersonDTO `json:"person" validate:"required"`
	// The list of bank accounts associated with the account holder
	BankAccounts []BankAccountDTO `json:"bankAccounts" validate:"required"`
	// The list of bank accounts known to and recognized by the account holder
	KnownBankAccounts []KnownBankAccountDTO `json:"knownBankAccounts" validate:"required"`
	// The creation timestamp of the account
	CreatedAt time.Time `json:"createdAt" validate:"required"`
}

// BankAccountDTO represents a bank account associated with an account holder
// @swagger:model BankAccountDTO
type BankAccountDTO struct {
	// The unique identifier of the account
	Id string `json:"id" validate:"required"`
	// The account number associated with the account
	AccountNumber string `json:"accountNumber" validate:"required"`
	// The type of the account (e.g., savings, checking)
	AccountType service.BankAccountType `json:"accountType" validate:"required"`
	// The available balance of the account. Valid to two decimal places.
	AvailableBalance string `json:"availableBalance" validate:"required"`
	// The pending balance of the account. Valid to two decimal places.
	PendingBalance string `json:"pendingBalance" validate:"required"`
}

// KnownBankAccountDTO represents an account known to and recognized by a particular account
// @swagger:model KnownBankAccountDTO
type KnownBankAccountDTO struct {
	// The account ID of the known account
	Id string `json:"id" validate:"required"`
	// The account number of the known account
	AccountNumber string `json:"accountNumber" validate:"required"`
	// The name of the account holder
	AccountHolder string `json:"accountHolder" validate:"required"`
	// The type of the account (e.g., savings, checking)
	AccountType string `json:"accountType" validate:"required"`
}

// PersonDTO represents an account holder
// @swagger:model PersonDTO
type PersonDTO struct {
	// The first name of the person
	FirstName string `json:"firstName" validate:"required"`
	// The last name of the person
	LastName string `json:"lastName" validate:"required"`
}

// AccountBalanceMonthDTO represents the series of account balances at the end of each month in the requested time period
// @swagger:model AccountBalanceMonthDTO
type AccountBalanceMonthDTO struct {
	// The month of the account balance
	Month int `json:"month" validate:"required"`
	// The year of the account balance
	Year int `json:"year" validate:"required"`
	// The available balance of the account at the end of the given month. Valid to two decimal places.
	AvailableBalance string `json:"availableBalance" validate:"required"`
	// The pending balance of the account at the end of the given month. Valid to two decimal places.
	PendingBalance string `json:"pendingBalance" validate:"required"`
}
