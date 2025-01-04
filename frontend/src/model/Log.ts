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

export enum Severity {
    INFO = "Info",
    ERROR = "Error",
    DEBUG = "Debug",
    WARNING = "Warning",
}