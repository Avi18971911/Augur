import NavigationForm from "../components/NavigationForm.tsx";
import {DataProvider} from "../provider/DataProvider.tsx";
import DataDisplay from "../components/DataDisplay.tsx";

function SearchPage() {
    return (
        <div>
            <div style={{ display: 'flex', flexDirection: 'row'}}>
                <DataProvider>
                    <div style={{ display: 'flex', flex: '0.8', paddingBottom: '25px' }}>
                        <NavigationForm />
                    </div>
                    <div style={{ position: 'relative', flex: '2', left: '-60px', minWidth: '50%', maxWidth: '70%'}}>
                        <DataDisplay />
                    </div>
                </DataProvider>
            </div>
        </div>
    )
}

export default SearchPage