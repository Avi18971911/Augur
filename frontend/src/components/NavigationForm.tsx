import {useState} from "react";
import {AnalyticsApi, Configuration, DataGetRequest, LogAndSpanType} from "../backend_api";

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
    const [searchStartTime, setSearchStartTime] = useState<string | undefined>(undefined)
    const [searchEndTime, setSearchEndTime] = useState<string | undefined>(undefined)
    const [searchLimit, setSearchLimit] = useState<number>(20)
    const [searchType, setSearchType] = useState<Type>(Type.ANY)
    const apiConfig = new Configuration({
        basePath: '/api'
    })
    const apiClient = new AnalyticsApi(apiConfig);

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
        apiClient.dataPost({searchParams: searchParams} as DataGetRequest)
            .then((response) => {
                console.log(response)
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
        <div style={{
            flexDirection: 'column', display: "flex", width: '100%', borderStyle: 'solid', borderColor: 'white',
            alignItems: 'center',
        }}>
            <h3>Navigation</h3>
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
                    style={{width: '50%', textIndent: '40%'}}
                    onChange={(e) => setSearchType(Type[e.target.value as keyof typeof Type])}
                >
                    {Object.values(Type).map((type) => (
                        <option value={type}>{type}</option>
                    ))}

                </select>

                <div
                    style={{marginTop: '20px'}}
                >
                    Start Time
                </div>
                <input
                    type={"datetime-local"} value={searchStartTime}
                    name={"startTime"} onChange={(e) => setSearchStartTime(e.target.value)}
                />

                <div
                    style={{marginTop: '20px', textAlign: 'center'}}
                >
                    End Time
                </div>
                <input
                    type={"datetime-local"} value={searchEndTime}
                    name={"endTime"} onChange={(e) => setSearchEndTime(e.target.value)}
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
    )
}

export default NavigationForm