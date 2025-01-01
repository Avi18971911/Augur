import {Log, LogOrSpan} from "../provider/DataProvider.tsx";
import React from "react";

type DataTableCellProps = {
    datum: LogOrSpan;
    expanded: boolean;
    onToggleExpand: (id: string) => void;
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

const DataTableCell: React.FC<DataTableCellProps> = ({ datum, expanded, onToggleExpand }) => {
    const details = getDetails(datum);
    return (
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
        </tr>
);
};

export default DataTableCell;