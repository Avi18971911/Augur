import React from "react";
import DataTableCell from "./DataTableCell.tsx";
import styles from "../styles/DataTable.module.css";
import {LogOrSpan} from "../model/LogOrSpan.ts";

type DataTableProps = {
    data: LogOrSpan[];
    showChainOfEvents: (id: string, clusterId: string) => void;
}

const DataTable: React.FC<DataTableProps> = ({ data, showChainOfEvents }) => {
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
                        <th>ChainOfEvents</th>
                    </tr>
                </thead>
                <tbody>
                    {data.map((datum) => (
                        <DataTableCell
                            key={datum.id}
                            datum={datum}
                            onToggleExpand={toggleExpand}
                            expanded={expandedRow === datum.id}
                            triggerChainOfEvents={showChainOfEvents}
                        />
                    ))}
                </tbody>
            </table>
        </div>
    );
}

export default DataTable;