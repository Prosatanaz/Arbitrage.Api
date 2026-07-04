import { useState } from 'react'
import './styles.css'
import MonitoringPage from './MonitoringPage'
import TradingPage from './TradingPage'
import ConnectorsPage from './ConnectorsPage'

type View = 'monitoring' | 'trading' | 'connectors'

const TABS: { view: View; label: string }[] = [
    { view: 'monitoring', label: 'Monitoring' },
    { view: 'trading', label: 'Trading' },
    { view: 'connectors', label: 'Connectors' },
]

function App() {
    const [view, setView] = useState<View>('monitoring')

    return (
        <>
            <nav className="nav-tabs">
                {TABS.map((tab) => (
                    <button
                        key={tab.view}
                        className={`nav-tab ${view === tab.view ? 'nav-tab--active' : ''}`}
                        onClick={() => setView(tab.view)}
                    >
                        {tab.label}
                    </button>
                ))}
            </nav>

            {view === 'monitoring' && <MonitoringPage />}
            {view === 'trading' && <TradingPage />}
            {view === 'connectors' && <ConnectorsPage />}
        </>
    )
}

export default App
