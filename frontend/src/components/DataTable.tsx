import {LogOrSpan} from "../provider/DataProvider.tsx";
import React from "react";
import DataTableCell from "./DataTableCell.tsx";
import styles from "../styles/DataTable.module.css";

type DataTableProps = {
    data: LogOrSpan[];
}

const DataTable: React.FC<DataTableProps> = ({ data }) => {
    return (
        <div className={styles.tableContainer}>
            <table className={styles.table}>
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
        </div>
    );
}

export default DataTable;