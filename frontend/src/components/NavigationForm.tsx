import {useState} from "react";

function NavigationForm() {
    const [selectedService, setSelectedService] = useState<string | undefined>(undefined)
    const [selectedOperation, setSelectedOperation] = useState<string | undefined>(undefined)
    const [searchStartTime, setSearchStartTime] = useState<string | undefined>(undefined)
    const [searchEndTime, setSearchEndTime] = useState<string | undefined>(undefined)

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
                    <option value={"service1"}>Placeholder Service</option>
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
                    <option value={"operation1"}>Placeholder Operation</option>
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
                    style={{marginTop: '20px'}}
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
                <input type={"number"} name={"limit"} />
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