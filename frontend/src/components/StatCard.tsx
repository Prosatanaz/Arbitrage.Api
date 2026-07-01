type StatCardProps = {
    title: string;
    value: string | number;
    subtitle?: string;
    tone?: 'default' | 'good' | 'warn' | 'bad';
};

export function StatCard({ title, value, subtitle, tone = 'default' }: StatCardProps) {
    return (
        <div className={`stat-card stat-card--${tone}`}>
            <div className="stat-card__title">{title}</div>
            <div className="stat-card__value">{value}</div>
            {subtitle && <div className="stat-card__subtitle">{subtitle}</div>}
        </div>
    );
}