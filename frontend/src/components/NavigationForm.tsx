import {useState} from "react";

function NavigationForm() {
    const [selectedService, setSelectedService] = useState("")
    const [selectedOperation, setSelectedOperation] = useState("")

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

                <div>
                    Operation
                </div>
                <select
                    value={selectedOperation}
                    onChange={(e) => setSelectedOperation(e.target.value)}
                >
                    <option value={"operation1"}>Placeholder Operation</option>
                </select>

                <div>
                    Limit Results
                </div>
                <input type={"number"} name={"limit"} />
            </form>

            <button type={"submit"} style={{marginTop: '20px', width: '50%'}}>Search</button>
        </div>
    )
}

export default NavigationForm