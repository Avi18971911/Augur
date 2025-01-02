import {useDataContext} from "../provider/DataProvider.tsx";
import DataTable from "./DataTable.tsx";



function DataDisplay() {
    const { data } = useDataContext()

    return (
        data.length?
            <div>
                <h2> Data </h2>
                <div>
                    <DataTable data={data} />
                </div>
            </div>
        :
            <div>
            </div>
    );
}

export default DataDisplay