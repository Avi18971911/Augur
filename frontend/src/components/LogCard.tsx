import React from 'react';
import {Log} from "../provider/DataProvider.tsx";

type LogCardProps = {
    log: Log;
};

const LogCard: React.FC<LogCardProps> = ({ log }) => {
    return (
        <div style={cardStyle}>
            <h3>Log ID: </h3> <p style={{fontSize: '6px'}}> {log.id} </p>
            <p>
                <strong>Timestamp:</strong> {log.timestamp.toLocaleString()}
            </p>
            <p>
                <strong>Severity:</strong> {log.severity}
            </p>
            <p>
                <strong>Message:</strong> {log.message}
            </p>
            <p>
                <strong>Service:</strong> {log.service}
            </p>
            <p>
                <strong>Trace ID:</strong> {log.traceId || 'N/A'}
            </p>
            <p>
                <strong>Cluster ID:</strong> {log.clusterId}
            </p>
        </div>
    );
};

const cardStyle: React.CSSProperties = {
    border: '1px solid #ddd',
    borderRadius: '8px',
    padding: '16px',
    marginBottom: '16px',
    backgroundColor: '#382e2e',
    boxShadow: '0px 2px 4px rgba(0, 0, 0, 0.1)',
};

export default LogCard;