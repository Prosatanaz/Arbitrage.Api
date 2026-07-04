import { useState } from 'react'
import './styles.css'
import Dashboard from './Dashboard'
import ConnectorsPage from './ConnectorsPage'

type View = 'dashboard' | 'connectors'

function App() {
    const [view, setView] = useState<View>('dashboard')

    return (
        <>
            <nav className="nav-tabs">
                <button
                    className={`nav-tab ${view === 'dashboard' ? 'nav-tab--active' : ''}`}
                    onClick={() => setView('dashboard')}
                >
                    Dashboard
                </button>
                <button
                    className={`nav-tab ${view === 'connectors' ? 'nav-tab--active' : ''}`}
                    onClick={() => setView('connectors')}
                >
                    Connectors
                </button>
            </nav>

            {view === 'dashboard' ? <Dashboard /> : <ConnectorsPage />}
        </>
    )
}

export default App
