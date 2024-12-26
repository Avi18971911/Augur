package handler

type LogAndSpanDTO struct {
	LogDTO  *LogDTO  `json:"log_dto,omitempty"`
	SpanDTO *SpanDTO `json:"span_dto,omitempty"`
}
