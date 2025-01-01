import {createContext, ReactNode, useContext, useState} from "react";
import {HandlerLogAndSpanDTO, HandlerLogDTO, HandlerSpanDTO, HandlerSpanEventDTO} from "../backend_api";

type DataContextType = {
    data: LogOrSpan[];
    setData: (data: HandlerLogAndSpanDTO[]) => void;
};

export type LogOrSpan = Log | Span;

export type Span = {
    id: string;
    createdAt: Date;
    spanId: string;
    parentSpanId: string;
    traceId: string;
    service: string;
    startTime: Date;
    endTime: Date;
    actionName: string;
    spanKind: string;
    clusterEvent: string;
    clusterId: string;
    attributes: Record<string, string> | undefined;
    events: SpanEvent[] | undefined;
    status: SpanStatus;
};

type SpanEvent = {
    name: string;
    attributes: Record<string, string>;
    timestamp: Date;
};

type SpanStatus = {
    message: string;
    code: SpanStatusCode;
};

enum SpanStatusCode {
    UNSET = "Unset",
    OK = "Ok",
    ERROR = "Error",
}

export type Log = {
    id: string;
    createdAt: Date;
    timestamp: Date;
    severity: Severity;
    message: string;
    service: string;
    traceId: string | undefined;
    spanId: string | undefined;
    clusterId: string;
};

enum Severity {
    INFO = "Info",
    ERROR = "Error",
    DEBUG = "Debug",
    WARNING = "Warning",
}

const mapSpanDTOToSpan = (spanDTO: HandlerSpanDTO): Span => {
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

const mapLogDtoToLog = (logDTO: HandlerLogDTO): Log => {
    console.log(logDTO);
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
    switch (severityDto) {
        case 'info':
            return Severity.INFO;
        case 'error':
            return Severity.ERROR;
        case 'debug':
            return Severity.DEBUG;
        case 'warning':
            return Severity.WARNING;
        default:
            throw new Error('Invalid severity ' + severityDto);
    }
}

const DataContext = createContext<DataContextType | undefined>(undefined);

export const useDataContext = () => {
    const context = useContext(DataContext);
    if (!context) {
        throw new Error('useDataContext must be used within a DataProvider');
    }
    return context;
};

export const DataProvider = ({ children }: { children: ReactNode }) => {
    const [data, setDataState] = useState<LogOrSpan[]>([]);

    const setData = (rawData: Array<HandlerLogAndSpanDTO>) => {
        const data = rawData.map((item) => {
            if (item.spanDto) {
                return mapSpanDTOToSpan(item.spanDto);
            } else if (item.logDto) {
                return mapLogDtoToLog(item.logDto);
            } else {
                throw new Error('Invalid data type');
            }
        });
        setDataState(data);
    }
    return (
        <DataContext.Provider value={{ data, setData }}>
            {children}
        </DataContext.Provider>
    );
};