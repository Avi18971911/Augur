import React from 'react';
import {Span} from "../provider/DataProvider.tsx";

type SpanCardProps = {
    span: Span;
};

const SpanCard: React.FC<SpanCardProps> = ({ span }) => {
    return (
        <div style={cardStyle}>
            <h3>Span ID: {span.spanId}</h3>
            <p>
                <strong>Trace ID:</strong> {span.traceId}
            </p>
            <p>
                <strong>Service:</strong> {span.service}
            </p>
            <p>
                <strong>Start Time:</strong> {span.startTime.toLocaleString()}
            </p>
            <p>
                <strong>End Time:</strong> {span.endTime.toLocaleString()}
            </p>
            <p>
                <strong>Action Name:</strong> {span.actionName}
            </p>
            <p>
                <strong>Cluster Event:</strong> {span.clusterEvent}
            </p>
            <p>
                <strong>Attributes:</strong>{' '}
                {span.attributes ? (
                    <ul>
                        {Object.entries(span.attributes).map(([key, value]) => (
                            <li key={key}>
                                {key}: {value}
                            </li>
                        ))}
                    </ul>
                ) : (
                    'N/A'
                )}
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

export default SpanCard;