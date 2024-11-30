package handler

import (
	"fake_svc/fake_server/pkg/service/model"
)

func accountDetailsToDTO(tx *model.AccountDetailsOutput) AccountDetailsResponseDTO {
	return AccountDetailsResponseDTO{
		Id:       tx.Id,
		Username: tx.Username,
		Person: PersonDTO{
			FirstName: tx.Person.FirstName,
			LastName:  tx.Person.LastName,
		},
		BankAccounts:      accountsToDTO(tx.BankAccounts),
		KnownBankAccounts: knownAccountToDTO(tx.KnownBankAccounts),
		CreatedAt:         tx.CreatedAt,
	}
}

func knownAccountToDTO(tx []model.KnownBankAccount) []KnownBankAccountDTO {
	knownAccountDTOList := make([]KnownBankAccountDTO, len(tx))
	for i, element := range tx {
		accountType := string(element.AccountType)
		knownAccountDTOList[i] = KnownBankAccountDTO{
			Id:            element.Id,
			AccountNumber: element.AccountNumber,
			AccountHolder: element.AccountHolder,
			AccountType:   accountType,
		}
	}
	return knownAccountDTOList
}

func accountsToDTO(tx []model.BankAccount) []BankAccountDTO {
	accountDTOList := make([]BankAccountDTO, len(tx))
	for i, element := range tx {
		accountDTOList[i] = BankAccountDTO{
			Id:               element.Id,
			AccountNumber:    element.AccountNumber,
			AccountType:      element.AccountType,
			PendingBalance:   element.PendingBalance.String(),
			AvailableBalance: element.AvailableBalance.String(),
		}
	}
	return accountDTOList
}
