import NavigationForm from "../components/NavigationForm.tsx";
import {DataProvider} from "../provider/DataProvider.tsx";

function SearchPage() {


    return (
        <div>
            <h1>Search Page</h1>
            <div style={{ display: 'flex', flexDirection: 'row'}}>
                <DataProvider>
                    <div style={{ display: 'flex', flex: '0.8', paddingBottom: '25px' }}>
                        <NavigationForm />
                    </div>
                    <div style={{flex: '2'}}>
                    </div>
                </DataProvider>
            </div>
        </div>
    )
}

export default SearchPage