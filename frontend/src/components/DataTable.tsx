import {LogOrSpan} from "../provider/DataProvider.tsx";
import React from "react";
import DataTableCell from "./DataTableCell.tsx";
import styles from "../styles/DataTable.module.css";

type DataTableProps = {
    data: LogOrSpan[];
}

const DataTable: React.FC<DataTableProps> = ({ data }) => {
    const [expandedRow, setExpandedRow] = React.useState<string | null>(null);

    const toggleExpand = (id: string) => {
        setExpandedRow((prev) => (prev === id ? null : id));
    };

    return (
        <div className={styles.tableContainer}>
            <table className={styles.table}>
                <thead>
                    <tr>
                        <th></th>
                        <th>Timestamp</th>
                        <th>Severity</th>
                        <th>Message</th>
                    </tr>
                </thead>
                <tbody>
                    {data.map((datum) => (
                        <DataTableCell
                            key={datum.id}
                            datum={datum}
                            onToggleExpand={toggleExpand}
                            expanded={expandedRow === datum.id}
                        />
                    ))}
                </tbody>
            </table>
        </div>
    );
}

export default DataTable;