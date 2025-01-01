import {Log, LogOrSpan} from "../provider/DataProvider.tsx";
import React from "react";

type DataTableCellProps = {
    datum: LogOrSpan;
};

type DatumDetails = {
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
            timestamp: datum.timestamp.toLocaleString(),
            severity: datum.severity,
            message: datum.message,
        };
    } else {
        return {
            timestamp: datum.startTime.toLocaleString(),
            severity: datum.status.code,
            message: datum.status.message,
        };
    }
}

const DataTableCell: React.FC<DataTableCellProps> = ({ datum }) => {
    const details = getDetails(datum);
    return (
        <tr>
            <td>{details.timestamp}</td>
            <td>{details.severity}</td>
            <td>{details.message}</td>
        </tr>
    );
};

export default DataTableCell;