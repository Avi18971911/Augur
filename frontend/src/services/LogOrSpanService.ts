import {HandlerLogDTO, HandlerSpanDTO, HandlerSpanEventDTO} from "../backend_api";
import {Span, SpanEvent, SpanStatus, SpanStatusCode} from "../model/Span.ts";
import {Log, Severity} from "../model/Log.ts";

export const mapSpanDTOToSpan = (spanDTO: HandlerSpanDTO): Span => {
    return {
        id: spanDTO.id,
        createdAt: new Date(spanDTO.createdAt),
        spanId: spanDTO.spanId,
        parentSpanId: spanDTO.parentSpanId,
        traceId: spanDTO.traceId,
        service: spanDTO.service,
        startTime: new Date(spanDTO.startTime),
        endTime: new Date(spanDTO.endTime),
        actionName: spanDTO.actionName,
        spanKind: spanDTO.spanKind,
        clusterEvent: spanDTO.clusterEvent,
        clusterId: spanDTO.clusterId,
        attributes: spanDTO.attributes,
        events: mapEventsDtoToEvents(spanDTO.events),
        status: mapSpanStatusDtoToSpanStatus(spanDTO),
    };
}

const mapEventsDtoToEvents = (eventsDto: Array<HandlerSpanEventDTO> | undefined): Array<SpanEvent> | undefined => {
    return eventsDto?.map((eventDto) => {
        return {
            name: eventDto.name,
            attributes: eventDto.attributes,
            timestamp: new Date(eventDto.timestamp),
        };
    });
}

const mapSpanStatusDtoToSpanStatus = (statusDto: any): SpanStatus => {
    switch (statusDto.code) {
        case SpanStatusCode.OK:
            return {
                message: statusDto.message,
                code: SpanStatusCode.OK,
            };
        case SpanStatusCode.ERROR:
            return {
                message: statusDto.message,
                code: SpanStatusCode.ERROR,
            };
        default:
            return {
                message: statusDto.message,
                code: SpanStatusCode.UNSET,
            };
    }
}

export const mapLogDtoToLog = (logDTO: HandlerLogDTO): Log => {
    return {
        id: logDTO.id,
        createdAt: new Date(logDTO.createdAt),
        timestamp: new Date(logDTO.timestamp),
        severity: mapSeverityDtoToSeverity(logDTO.severity),
        message: logDTO.message,
        service: logDTO.service,
        traceId: logDTO.traceId,
        spanId: logDTO.spanId,
        clusterId: logDTO.clusterId,
    };
}

const mapSeverityDtoToSeverity = (severityDto: string): Severity => {
    switch (severityDto.toLowerCase()) {
        case Severity.INFO.toLowerCase():
            return Severity.INFO;
        case Severity.ERROR.toLowerCase():
            return Severity.ERROR;
        case Severity.DEBUG.toLowerCase():
            return Severity.DEBUG;
        case Severity.WARNING.toLowerCase():
            return Severity.WARNING;
        default:
            throw new Error('Invalid severity ' + severityDto);
    }
}