import {useState} from "react";

enum Type {
    ANY = "Any",
    INFO = "Info",
    ERROR = "Error",
    WARNING = "Warning",
    OK = "OK",
    UNASSIGNED = "Unassigned"
}

function NavigationForm() {
    const [selectedService, setSelectedService] = useState<string>("All")
    const [selectedOperation, setSelectedOperation] = useState<string>("All")
    const [searchStartTime, setSearchStartTime] = useState<string | undefined>(undefined)
    const [searchEndTime, setSearchEndTime] = useState<string | undefined>(undefined)
    const [searchLimit, setSearchLimit] = useState<number>(20)
    const [searchType, setSearchType] = useState<Type>(Type.ANY)


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
                    <option value={"All"}>All Possible Services</option>
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
                    <option value={"All"}>All Possible Operations</option>
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
            >
                Search
            </button>
        </div>
    )
}

export default NavigationForm