package log_and_span

func getLogsAndSpansQuery(params SearchParams) map[string]interface{} {
	var shouldClauses []map[string]interface{}
	var filterClauses []map[string]interface{}

	var logMustClauses []map[string]interface{}
	logRange := map[string]interface{}{}

	if params.StartTime != nil || params.EndTime != nil {
		if params.StartTime != nil {
			logRange["gte"] = *params.StartTime
		}
		if params.EndTime != nil {
			logRange["lte"] = *params.EndTime
		}
		logMustClauses = append(logMustClauses, map[string]interface{}{
			"range": map[string]interface{}{
				"timestamp": logRange,
			},
		})
	}

	var spanMustClauses []map[string]interface{}

	if params.StartTime != nil || params.EndTime != nil {
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
	}

	if params.Types != nil && len(params.Types) > 0 {
		spanMustClauses = append(spanMustClauses, map[string]interface{}{
			"terms": map[string]interface{}{
				"status.code": params.Types,
			},
		})

		logMustClauses = append(logMustClauses, map[string]interface{}{
			"terms": map[string]interface{}{
				"severity": params.Types,
			},
		})
	}

	if params.Operation != nil {
		spanMustClauses = append(spanMustClauses, map[string]interface{}{
			"term": map[string]interface{}{
				"action_name": *params.Operation,
			},
		})
	}

	logCondition := map[string]interface{}{
		"bool": map[string]interface{}{
			"must": logMustClauses,
		},
	}

	spanCondition := map[string]interface{}{
		"bool": map[string]interface{}{
			"must": spanMustClauses,
		},
	}

	shouldClauses = append(shouldClauses, logCondition)
	shouldClauses = append(shouldClauses, spanCondition)

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
