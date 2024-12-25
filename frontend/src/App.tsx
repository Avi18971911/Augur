import './App.css'
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom'
import Toolbar from "./components/Toolbar.tsx";
import SearchPage from "./pages/SearchPage.tsx";

function App() {
    return (
        <Router>
            <Toolbar/>
            <div style={{width: '100%', padding: '0px', marginBottom: 0}}>
                <Routes>
                    <Route path="/" element={<SearchPage/>}/>
                </Routes>
            </div>
        </Router>
)
    ;
}

export default App
