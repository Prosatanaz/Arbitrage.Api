export type SystemHealthResponse = {
    status: string;
    now: string;
    universe?: {
        loaded: boolean;
        pairCount: number;
        outputPath: string;
        error?: string | null;
    };
    bbo?: {
        totalSnapshots: number;
        connectorCount: number;
        freshCount: number;
    };
    depth?: {
        totalTargets: number;
        totalCachedSnapshots: number;
        targetsRecentlyChanged: boolean;
    };
    opportunities?: {
        hasData: boolean;
        count: number;
        validCount: number;
        positiveValidCount: number;
    };
    degradedConnectors?: string[];
};

export type ActiveSignalsSummaryResponse = {
    updatedAt: string | null;
    hasData: boolean;
    total: number;
    valid: number;
    alerts: number;
    candidates: number;
    ignored: number;
    blocked: number;
    invalid: number;
    bestAlert: BestSignalSummary | null;
    bestCandidate: BestSignalSummary | null;
};

export type BestSignalSummary = {
    tradingPair: string;
    direction: string;
    netEdgePct: number | null;
    decisionReason: string;
};

export type ActiveSignalsResponse = {
    updatedAt: string | null;
    hasData: boolean;
    count: number;
    items: ActiveSignalItem[];
};

export type ActiveSignalItem = {
    tradingPair: string;
    direction: string;
    longConnector: string;
    shortConnector: string;

    decision: string;
    decisionReason: string;
    validationStatus?: string;
    validationReason?: string | null;

    notionalUsd: number;

    grossSpreadPct: number | null;
    estimatedFeesPct: number | null;
    netEdgePct: number | null;

    buyAveragePrice: number | null;
    sellAveragePrice: number | null;
    buySlippagePct: number | null;
    sellSlippagePct: number | null;
    buyLevelsUsed: number | null;
    sellLevelsUsed: number | null;

    detectedAt: string;
    validatedAt: string | null;
    signalAgeMs: number;
};

export type AnalyticsSummaryResponse = {
    tradesCount: number;
    avgEntryEdgePct: number;
    maxEntryEdgePct: number;
    minEntryEdgePct: number;
    firstEntryAt: string | null;
    lastExitOrSeenAt: string | null;
};

export type TopOpportunityItem = {
    tradingPair: string;
    longConnector: string;
    shortConnector: string;
    trades: number;
    avgEntryEdgePct: number;
    maxEntryEdgePct: number;
    minEntryEdgePct: number;
    firstEntryAt: string;
    lastSeenAt: string;
};

export type DepthIssueItem = {
    problematicConnector: string;
    status: string;
    rows: number;
};

export type CarryTradeItem = {
    id: string;
    tradingPair: string;
    longConnector: string;
    shortConnector: string;
    notionalUsd: number;
    baseQuantity: number;
    entryLongPrice: number;
    entryShortPrice: number;
    entryFeesUsd: number;
    entryNetEdgePct: number;
    openedAt: string;
    status: string;
    exitLongPrice: number | null;
    exitShortPrice: number | null;
    exitFeesUsd: number | null;
    closeReason: string | null;
    closedAt: string | null;
    realizedPnlUsd: number | null;
    error: string | null;
};

export type RealizedSummaryResponse = {
    tradesCount: number;
    totalRealizedPnlUsd: number;
    avgRealizedPnlUsd: number;
    winRatePct: number;
    avgHoldMinutes: number;
    lastClosedAt: string | null;
};