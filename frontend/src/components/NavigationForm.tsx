import {useState} from "react";

function NavigationForm() {
    const [selectedService, setSelectedService] = useState("")

    return (
        <div style={{flexDirection: 'column', display: "flex"}}>
            <h3>Navigation</h3>
            <form action={"/search"} method={"post"} style={{flexDirection: 'column'}}>
                <div>
                    Service
                </div>
                <select
                    value={selectedService}
                    onChange={(e) => setSelectedService(e.target.value)}
                >
                    <option value={"service1"}>Service 1</option>
                    <option value={"service2"}>Service 2</option>
                    <option value={"service3"}>Service 3</option>
                </select>
            </form>
        </div>
    )
}

export default NavigationForm