import NavigationForm from "../components/NavigationForm.tsx";
import {DataProvider} from "../provider/DataProvider.tsx";
import DataDisplay from "../components/DataDisplay.tsx";

function SearchPage() {
    return (
        <div>
            <div style={{ display: 'flex', flexDirection: 'row', fontSize: '1.2vw'}}>
                <DataProvider>
                    <div style={{ display: 'flex', flex: '0.2', paddingBottom: '25px' }}>
                        <NavigationForm />
                    </div>
                    <div style={{ position: 'relative', flex: '2', left: '30px', minWidth: '50%', maxWidth: '70%'}}>
                        <DataDisplay />
                    </div>
                </DataProvider>
            </div>
        </div>
    )
}

export default SearchPage