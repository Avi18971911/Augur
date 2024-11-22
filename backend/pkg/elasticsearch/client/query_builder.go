package client

func buildSearchWithPitQuery(
	query map[string]interface{},
	pitId string,
	filterParams []map[string]string,
) map[string]interface{} {
	query["pit"] = map[string]string{
		"id": pitId,
	}
	if len(filterParams) > 0 {
		query["filter"] = filterParams
	}
	return query
}
