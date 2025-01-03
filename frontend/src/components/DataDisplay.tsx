import DataTable from "./DataTable.tsx";
import React from "react";
import {useDataContext} from "../provider/DataProvider.tsx";
import {useApiClientContext} from "../provider/ApiClientProvider.tsx";
import {GraphPostRequest, HandlerChainOfEventsRequestDTO} from "../backend_api";



function DataDisplay() {
    const apiClient = useApiClientContext();
    const { data } = useDataContext()
    const [shouldShowChainOfEvents, setShowChainOfEvents] = React.useState<boolean>(false);

    function showChainOfEvents(id: string) {
        setShowChainOfEvents(true);
        const payload: GraphPostRequest = {
            logOrSpanData: {
                id: id,
            }
        }
        apiClient.graphPost(payload)
            .then((response) => {
              console.log(response)
            })
            .catch((error) => {
                console.error(error)
            })
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