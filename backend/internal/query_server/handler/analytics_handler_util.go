package handler

import (
	"errors"
	spanModel "github.com/Avi18971911/Augur/internal/otel_server/trace/model"
	"github.com/Avi18971911/Augur/internal/query_server/service/inference/model"
)

func mapChainOfEventsResponseToDTO(mleSequence map[string]*model.ClusterNode) ChainOfEventsResponseDTO {
	// TODO: Reconcile the fact that there may not be log or span data
	graph := make(map[string]ChainOfEventsNodeDTO)
	for _, node := range mleSequence {
		successors := make([]EdgeDTO, len(node.Successors))
		for i, successor := range node.Successors {
			if mleSequence[successor.ClusterId] == nil {
				continue
			}
			successors[i].Id = mleSequence[successor.ClusterId].LogOrSpanData.Id
			successors[i].TDOA = successor.TDOA
		}
		predecessors := make([]EdgeDTO, len(node.Predecessors))
		for i, predecessor := range node.Predecessors {
			if mleSequence[predecessor.ClusterId] == nil {
				continue
			}
			predecessors[i].Id = mleSequence[predecessor.ClusterId].LogOrSpanData.Id
			predecessors[i].TDOA = predecessor.TDOA
		}
		var spanDTO = SpanDTO{}
		var logDTO = LogDTO{}
		if node.LogOrSpanData.SpanDetails != nil {
			spanDTO = toSpanDTO(node)
		} else {
			logDTO = toLogDTO(node)
		}

		graph[node.LogOrSpanData.Id] = ChainOfEventsNodeDTO{
			Id:           node.LogOrSpanData.Id,
			ClusterId:    node.LogOrSpanData.ClusterId,
			Successors:   successors,
			Predecessors: predecessors,
			SpanDTO:      &spanDTO,
			LogDTO:       &logDTO,
		}
	}
	return ChainOfEventsResponseDTO{
		Graph: graph,
	}
}

func toSpanDTO(node *model.ClusterNode) SpanDTO {
	return SpanDTO{
		Id:           node.LogOrSpanData.SpanDetails.Id,
		CreatedAt:    node.LogOrSpanData.SpanDetails.CreatedAt,
		SpanID:       node.LogOrSpanData.SpanDetails.SpanID,
		ParentSpanID: node.LogOrSpanData.SpanDetails.ParentSpanID,
		TraceID:      node.LogOrSpanData.SpanDetails.TraceID,
		Service:      node.LogOrSpanData.SpanDetails.Service,
		StartTime:    node.LogOrSpanData.SpanDetails.StartTime,
		EndTime:      node.LogOrSpanData.SpanDetails.EndTime,
		ActionName:   node.LogOrSpanData.SpanDetails.ActionName,
		SpanKind:     node.LogOrSpanData.SpanDetails.SpanKind,
		ClusterEvent: node.LogOrSpanData.SpanDetails.ClusterEvent,
		ClusterId:    node.LogOrSpanData.SpanDetails.ClusterId,
		Attributes:   node.LogOrSpanData.SpanDetails.Attributes,
		Events:       mapModelToSpanEventDTO(node.LogOrSpanData.SpanDetails.Events),
	}
}

func toLogDTO(node *model.ClusterNode) LogDTO {
	return LogDTO{
		Id:        node.LogOrSpanData.LogDetails.Id,
		CreatedAt: node.LogOrSpanData.LogDetails.CreatedAt,
		Timestamp: node.LogOrSpanData.LogDetails.Timestamp,
		Severity:  string(node.LogOrSpanData.LogDetails.Severity),
		Message:   node.LogOrSpanData.LogDetails.Message,
		Service:   node.LogOrSpanData.LogDetails.Service,
		TraceId:   node.LogOrSpanData.LogDetails.TraceId,
		SpanId:    node.LogOrSpanData.LogDetails.SpanId,
		ClusterId: node.LogOrSpanData.LogDetails.ClusterId,
	}
}

func mapModelToSpanEventDTO(events []spanModel.SpanEvent) []SpanEventDTO {
	var dto []SpanEventDTO
	for _, event := range events {
		dto = append(dto, SpanEventDTO{
			Name:       event.Name,
			Attributes: event.Attributes,
			Timestamp:  event.Timestamp,
		})
	}
	return dto
}

var (
	ErrNoLogOrSpanData = errors.New("no log or span data provided")
	ErrNoId            = errors.New("no ID provided")
	ErrNoClusterId     = errors.New("no cluster ID provided")
)
