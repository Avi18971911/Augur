package service

func getAllDocumentsQuery() map[string]interface{} {
	return map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}
}
