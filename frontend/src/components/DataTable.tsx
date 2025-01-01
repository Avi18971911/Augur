import {LogOrSpan} from "../provider/DataProvider.tsx";
import React from "react";
import DataTableCell from "./DataTableCell.tsx";

type DataTableProps = {
    data: LogOrSpan[];
}

const DataTable: React.FC<DataTableProps> = ({ data }) => {
    return (
        <table>
            <thead>
                <tr>
                    <th>Timestamp</th>
                    <th>Severity</th>
                    <th>Message</th>
                </tr>
            </thead>
            <tbody>
                {data.map((datum) => (
                    <DataTableCell key={datum.id} datum={datum} />
                ))}
            </tbody>
        </table>
    );
}

export default DataTable;