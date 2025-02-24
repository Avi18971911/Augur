import {useState} from "react";
import {DataPostRequest, LogAndSpanType} from "../backend_api";
import {useApiClientContext} from "../provider/ApiClientProvider.tsx";
import {useDataContext} from "../provider/DataProvider.tsx";

enum Type {
    ANY = "Any",
    INFO = "Info",
    ERROR = "Error",
    WARNING = "Warning",
    OK = "OK",
    UNASSIGNED = "Unassigned"
}

const allServices = "allServices"
const allOperations = "allOperations"

function NavigationForm() {
    const [selectedService, setSelectedService] = useState<string>(allServices)
    const [selectedOperation, setSelectedOperation] = useState<string>(allOperations)
    const [searchStartTime, setSearchStartTime] = useState<string>(
        new Date("2021-01-01").toISOString().slice(0, 16) // To match the datetime-local input format
    )
    const [searchEndTime, setSearchEndTime] = useState<string>(
        new Date().toISOString().slice(0, 16) // To match the datetime-local input format
    )
    const [searchLimit, setSearchLimit] = useState<number>(20)
    const [searchType, setSearchType] = useState<Type>(Type.ANY)

    const apiClient = useApiClientContext()
    const { setData } = useDataContext()

    // TODO: Move these functions to another component or service
    function getLogsAndSpans(
        service: string,
        operation: string,
        startTime: string | undefined,
        endTime: string | undefined,
        resultLimit: number,
        type: Type
    ) {
        const searchParams = {
            service: (service == allServices) ? null : service,
            operation: (operation == allOperations) ? null : operation,
            startTime: startTime || null,
            endTime: endTime || null,
            type: convertType([type]) || null,
        }
        apiClient.dataPost({searchParams: searchParams} as DataPostRequest)
            .then((response) => {
                if (response.data === undefined) {
                    return
                }
                setData(response.data)
            })
            .catch((error) => {
                console.error(error)
            })
    }

    function convertType(typeList: Array<Type>): Array<LogAndSpanType> | null {
        if (typeList.length === 0 || typeList.includes(Type.ANY)) {
            return null;
        }

        const typeMap: Record<Type, LogAndSpanType> = {
            [Type.INFO]: LogAndSpanType.Info,
            [Type.ERROR]: LogAndSpanType.Error,
            [Type.WARNING]: LogAndSpanType.Warn,
            [Type.OK]: LogAndSpanType.Ok,
            [Type.UNASSIGNED]: LogAndSpanType.Unset,
            // the following will never be used
            [Type.ANY]: LogAndSpanType.Info,
        };

        return typeList.map((type) => typeMap[type]);
    }
    return (
        <div style={{display: 'flex', flexDirection: 'column', flex: '1.0'}}>
            <h2>Navigation</h2>
            <div style={{
                flexDirection: 'column', display: "flex", width: '100%', borderStyle: 'solid', borderColor: 'white',
                alignItems: 'center', padding: '20px', marginTop: '20px'
            }}>
                <form action={"/search"} method={"post"} style={{flexDirection: 'column'}}>
                    <div>
                        Service
                    </div>
                    <select
                        value={selectedService}
                        onChange={(e) => setSelectedService(e.target.value)}
                    >
                        <option value={allServices}>All Possible Services</option>
                    </select>

                    <div
                        style={{marginTop: '20px'}}
                    >
                        Operation
                    </div>
                    <select
                        value={selectedOperation}
                        onChange={(e) => setSelectedOperation(e.target.value)}
                    >
                        <option value={allOperations}>All Possible Operations</option>
                    </select>

                    <div
                        style={{marginTop: '20px'}}
                    >
                        Type
                    </div>
                    <select
                        value={searchType}
                        style={{width: '70%'}}
                        onChange={(e) => {
                            const matchingType = Object.values(Type).find(
                                (type) => type === e.target.value
                            ) ?? Type.ANY;
                            setSearchType(matchingType);
                        }}
                    >
                        {Object.values(Type).map((type) => (
                            <option key={type} value={type}>{type}</option>
                        ))}

                    </select>

                    <div
                        style={{marginTop: '20px'}}
                    >
                        Start Time
                    </div>
                    <input
                        type={"datetime-local"} value={searchStartTime}
                        name={"startTime"} onChange={
                            (e) => setSearchStartTime(e.target.value)
                        }
                    />

                    <div
                        style={{marginTop: '20px', textAlign: 'center'}}
                    >
                        End Time
                    </div>
                    <input
                        type={"datetime-local"} value={searchEndTime}
                        name={"endTime"} onChange={
                            (e) => setSearchEndTime(e.target.value)
                        }
                    />

                    <div
                        style={{marginTop: '20px'}}
                    >
                        Limit Results
                    </div>
                    <input
                        type={"number"} name={"limit"} value={searchLimit} style={{textAlign: 'center', width: '30%'}}
                        onChange={(e) => setSearchLimit(parseInt(e.target.value))}
                    />

                </form>

                <button
                    type={"submit"}
                    style={{marginTop: '20px', width: '50%', marginBottom: '20px', background: 'gray'}}
                    onClick={() => {
                        getLogsAndSpans(
                            selectedService,
                            selectedOperation,
                            searchStartTime,
                            searchEndTime,
                            searchLimit,
                            searchType,
                        )
                    }}
                >
                    Search
                </button>
            </div>
        </div>
    )
}

export default NavigationForm