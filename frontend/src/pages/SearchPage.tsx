import NavigationForm from "../components/NavigationForm.tsx";

function SearchPage() {
    return (
        <div style={{width: '100%'}}>
            <h1>Search Page</h1>
            <div style={{ display: 'flex', flexDirection: 'row'}}>
                <div style={{ display: 'flex', gap: '1rem', flex: '0.8' }}>
                    <NavigationForm />
                </div>
                <div style={{flex: '2'}}>
                </div>
            </div>
        </div>
    )
}

export default SearchPage