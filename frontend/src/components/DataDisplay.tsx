import DataTable from "./DataTable.tsx";
import React from "react";
import {useDataContext} from "../provider/DataProvider.tsx";



function DataDisplay() {
    const { data } = useDataContext()
    const [shouldShowChainOfEvents, setShowChainOfEvents] = React.useState<boolean>(false);
    function showChainOfEvents(id: string) {
        setShowChainOfEvents(true);
    }

    return (
        data.length?
            <div>
                <h2> Data </h2>
                <div>
                    <DataTable data={data} showChainOfEvents={showChainOfEvents} />
                </div>
            </div>
        :
            <div>
            </div>
    );
}

export default DataDisplay