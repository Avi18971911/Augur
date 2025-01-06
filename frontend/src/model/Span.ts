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

export type SpanEvent = {
    name: string;
    attributes: Record<string, string>;
    timestamp: Date;
};

export type SpanStatus = {
    message: string;
    code: SpanStatusCode;
};

export enum SpanStatusCode {
    UNSET = "Unset",
    OK = "Ok",
    ERROR = "Error",
}