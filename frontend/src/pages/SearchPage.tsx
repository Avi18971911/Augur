import NavigationForm from "../components/NavigationForm.tsx";

function SearchPage() {
    return (
        <div>
            <h1>Search Page</h1>
            <div style={{ display: 'flex', flexDirection: 'row'}}>
                <div style={{ display: 'flex', gap: '1rem' }}>
                    <NavigationForm />
                </div>
                <div>
                    Hey
                </div>
            </div>
        </div>
    )
}

export default SearchPage