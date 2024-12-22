import './App.css'
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom'
import Toolbar from "./components/Toolbar.tsx";
import SearchPage from "./pages/SearchPage.tsx";

function App() {
    return (
        <Router>
            <Toolbar />
            <Routes>
                <Route path="/" element={<SearchPage />} />
            </Routes>
        </Router>
    );
}

export default App
