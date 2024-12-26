package log_and_span

func getAllErrorsQuery(params ErrorSearchParams) map[string]interface{} {
	var shouldClauses []map[string]interface{}
	var filterClauses []map[string]interface{}

	if params.StartTime != nil || params.EndTime != nil {
		logRange := map[string]interface{}{}
		if params.StartTime != nil {
			logRange["gte"] = *params.StartTime
		}
		if params.EndTime != nil {
			logRange["lte"] = *params.EndTime
		}

		logCondition := map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"range": map[string]interface{}{
							"timestamp": logRange,
						},
					},
					{
						"term": map[string]interface{}{
							"severity": "error",
						},
					},
				},
			},
		}
		shouldClauses = append(shouldClauses, logCondition)
	}

	if params.StartTime != nil || params.EndTime != nil {
		spanMustClauses := []map[string]interface{}{}
		if params.StartTime != nil {
			spanMustClauses = append(spanMustClauses, map[string]interface{}{
				"range": map[string]interface{}{
					"start_time": map[string]interface{}{
						"lte": *params.EndTime,
					},
				},
			})
		}
		if params.EndTime != nil {
			spanMustClauses = append(spanMustClauses, map[string]interface{}{
				"range": map[string]interface{}{
					"end_time": map[string]interface{}{
						"gte": *params.StartTime,
					},
				},
			})
		}
		spanMustClauses = append(spanMustClauses, map[string]interface{}{
			"term": map[string]interface{}{
				"status": "error",
			},
		})

		spanCondition := map[string]interface{}{
			"bool": map[string]interface{}{
				"must": spanMustClauses,
			},
		}
		shouldClauses = append(shouldClauses, spanCondition)
	}

	if params.Service != nil {
		filterClauses = append(filterClauses, map[string]interface{}{
			"term": map[string]interface{}{
				"service": *params.Service,
			},
		})
	}

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"should":               shouldClauses,
				"minimum_should_match": 1,
				"filter":               filterClauses,
			},
		},
	}

	return query
}
