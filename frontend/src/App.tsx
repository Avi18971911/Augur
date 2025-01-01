import './App.css'
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom'
import Toolbar from "./components/Toolbar.tsx";
import SearchPage from "./pages/SearchPage.tsx";
import {ApiClientProvider} from "./provider/ApiClientProvider.tsx";

function App() {
    return (
        <Router>
            <ApiClientProvider>
                <div style={{position: 'absolute', top: '0px', width: '100%', left: '0px'}}>
                    <Toolbar/>
                    <div style={{width: '90%', position: 'relative', top: '40px', left: '40px'}}>
                        <Routes>
                            <Route path="/" element={<SearchPage/>}/>
                        </Routes>
                    </div>
                </div>
            </ApiClientProvider>
        </Router>
    )
}

export default App
