package handler

// LogAndSpanDTO represents the details of a log or span
// @swagger:model LogAndSpanDTO
type LogAndSpanDTO struct {
	// The log details, null if the error is a span
	LogDTO *LogDTO `json:"log_dto,omitempty"`
	// The span details, null if the error is a log
	SpanDTO *SpanDTO `json:"span_dto,omitempty"`
}

// ErrorResponseDTO represents the response to an error request
// @swagger:model ErrorResponseDTO
type ErrorResponseDTO struct {
	// The log or span representing the error
	Errors []LogAndSpanDTO `json:"errors"`
}
