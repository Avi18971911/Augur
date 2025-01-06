import DataTable from "./DataTable.tsx";
import React from "react";
import {useDataContext} from "../provider/DataProvider.tsx";
import {useApiClientContext} from "../provider/ApiClientProvider.tsx";
import {GraphPostRequest} from "../backend_api";
import {mapChainOfEventsResponseToChainOfEventsGraph} from "../services/ChainOfEventsService.ts";
import type {ChainOfEventsGraph as ChainOfEventsGraphModel} from "../model/ChainOfEventsGraph.ts";
import ChainOfEventsGraph from "./ChainOfEventsGraph.tsx";



function DataDisplay() {
    const apiClient = useApiClientContext();
    const { data } = useDataContext()
    const [chainOfEventsGraph, setChainOfEventsGraph] = React.useState<ChainOfEventsGraphModel | undefined>(undefined);

    function setChainOfEvents(id: string) {
        const payload: GraphPostRequest = {
            logOrSpanData: {
                id: id,
            }
        }
        apiClient.graphPost(payload)
            .then((response) => {
                console.log(response)
                const graph = mapChainOfEventsResponseToChainOfEventsGraph(response, id)
                setChainOfEventsGraph(graph)
            })
            .catch((error) => {
                console.error(error)
            })
    }

    return (
        <>
            {
                data.length ?
                    <div>
                        <h2> Data </h2>
                        <div>
                            <DataTable data={data} showChainOfEvents={setChainOfEvents}/>
                        </div>
                    </div>
                :
                    <div>
                        <h2> No Data </h2>
                    </div>
            }

            {
                chainOfEventsGraph ?
                    <div>
                        <h2> Chain of Events </h2>
                        <ChainOfEventsGraph chainOfEvents={chainOfEventsGraph!} />
                    </div>
                :
                    <div>
                        <h2> No Chain of Events </h2>
                    </div>
            }
        </>
    );
}

export default DataDisplay