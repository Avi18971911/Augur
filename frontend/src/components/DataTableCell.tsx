import React from "react";
import {Log, LogOrSpan} from "../provider/DataProvider.tsx";

type DataTableCellProps = {
    datum: LogOrSpan;
    expanded: boolean;
    onToggleExpand: (id: string) => void;
    triggerChainOfEvents: (id: string) => void;
};

type DatumDetails = {
    id: string;
    timestamp: string;
    severity: string;
    message: string;
}

const isLog = (datum: LogOrSpan): datum is Log => {
    return (datum as Log).severity !== undefined;
}

function getDetails(datum: LogOrSpan): DatumDetails {
    if (isLog(datum)) {
        return {
            id: datum.id,
            timestamp: datum.timestamp.toLocaleString(),
            severity: datum.severity,
            message: datum.message,
        };
    } else {
        return {
            id: datum.id,
            timestamp: datum.startTime.toLocaleString(),
            severity: datum.status.code,
            message: datum.status.message,
        };
    }
}

const DataTableCell: React.FC<DataTableCellProps> = ({ datum, expanded, onToggleExpand, triggerChainOfEvents }) => {
    const details = getDetails(datum);
    return (
        <>
            <tr>
                <td>
                    <button
                        onClick={() => onToggleExpand(details.id)}
                        style={{
                            background: 'none',
                            border: 'none',
                            cursor: 'pointer',
                            color: 'white',
                            fontSize: '16px',
                        }}
                    >
                        {expanded ? '▼' : '▶'}
                    </button>
                </td>
                <td>{details.timestamp}</td>
                <td>{details.severity}</td>
                <td>{details.message}</td>
                <td>
                    <button
                        onClick={() => triggerChainOfEvents(details.id)}
                        style={{
                            background: 'linear-gradient(135deg, #777, #333)',
                            cursor: 'pointer',
                            color: 'white',
                            fontSize: '16px',
                            border: 'none',
                            borderRadius: '8px',
                            padding: '8px 16px',
                            boxShadow: '0px 4px 6px rgba(0, 0, 0, 0.1)',
                            transition: 'transform 0.2s ease, background 0.3s ease',
                        }}
                        onMouseOver={(e) => (
                            e.currentTarget.style.background = '#7a7979'
                        )}
                        onMouseOut={(e) =>
                            (e.currentTarget.style.background = 'linear-gradient(135deg, #777, #333)')
                        }
                        onMouseDown={(e) => {
                            e.currentTarget.style.transform = 'scale(0.95)';
                            e.currentTarget.style.boxShadow = '0px 2px 4px rgba(0, 0, 0, 0.2)';
                        }}
                        onMouseUp={(e) => {
                            e.currentTarget.style.transform = 'scale(1)';
                            e.currentTarget.style.boxShadow = '0px 4px 6px rgba(0, 0, 0, 0.1)';
                        }}
                    >
                        Show CoE
                    </button>
                </td>
            </tr>

            {expanded && (
                <tr>
                    <td colSpan={4}>
                        <pre>
                            {JSON.stringify(datum, null, 2)}
                        </pre>
                    </td>
                </tr>
            )}
        </>
    );
};

export default DataTableCell;