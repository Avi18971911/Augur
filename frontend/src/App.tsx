import './App.css'
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom'
import Toolbar from "./components/Toolbar.tsx";
import SearchPage from "./pages/SearchPage.tsx";

function App() {
    return (
        <Router>
            <div style={{position: 'fixed', top: '0px', width: '100%', left: '0px', zIndex: '9999'}}>
                <Toolbar/>
            </div>
            <div style={{width: '100%', position: 'absolute', top: '20px'}}>
                <Routes>
                    <Route path="/" element={<SearchPage/>}/>
                </Routes>
            </div>
        </Router>
    )
}

export default App
