export const dynamic = "force-dynamic";

type MoversRow = {
  ticker: string;
  mentions: number;
  news_mentions: number;
  reddit_mentions: number;
  sentiment_mean: number;
  bullish_ratio: number;
  conviction_mean: number;
  baseline_per_hour: number;
  spike_ratio: number;
  cross_source: boolean;
};

async function fetchJSON<T>(path: string): Promise<T> {
  const res = await fetch(path, { cache: "no-store" });
  if (!res.ok) throw new Error(`${path} failed: ${res.status}`);
  return res.json();
}

export default async function Home() {
  const health = await fetchJSON<{ status: string }>("/api/health");
  const movers = await fetchJSON<{ results: MoversRow[] }>(
    "/api/tickers/movers?window_minutes=60&baseline_hours=6&limit=15&require_cross_source=false",
  );

  return (
    <main className="min-h-screen bg-zinc-950 text-zinc-100">
      <div className="mx-auto max-w-6xl px-6 py-10">
        <div className="flex items-start justify-between gap-6">
          <div>
            <h1 className="text-3xl font-semibold tracking-tight">
              Stock Emotional Analysis
            </h1>
            <p className="mt-2 text-zinc-300">
              Live movers ranked from Reddit + News sentiment pipeline.
            </p>
          </div>

          <div className="rounded-lg border border-zinc-800 bg-zinc-900 px-4 py-2 text-sm">
            <span className="mr-2 text-zinc-400">API</span>
            <span
              className={
                health.status === "ok" ? "text-emerald-400" : "text-red-400"
              }
            >
              {health.status}
            </span>
          </div>
        </div>

        <div className="mt-8 rounded-xl border border-zinc-800 bg-zinc-900/40">
          <div className="border-b border-zinc-800 px-5 py-4">
            <div className="flex items-center justify-between">
              <h2 className="text-lg font-medium">Top Movers</h2>
              <a
                href="/tech"
                className="text-sm text-zinc-300 underline hover:text-zinc-100"
              >
                Tech / How it works
              </a>
            </div>
            <p className="mt-1 text-sm text-zinc-400">
              window=60m, baseline=6h (MVP scoring). Click Tech to see the
              pipeline.
            </p>
          </div>

          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="text-zinc-300">
                <tr className="text-left">
                  <th className="px-5 py-3">Ticker</th>
                  <th className="px-5 py-3">Mentions</th>
                  <th className="px-5 py-3">Spike</th>
                  <th className="px-5 py-3">Bullish</th>
                  <th className="px-5 py-3">Sentiment</th>
                  <th className="px-5 py-3">Conviction</th>
                  <th className="px-5 py-3">Sources</th>
                </tr>
              </thead>
              <tbody>
                {movers.results.map((r) => (
                  <tr key={r.ticker} className="border-t border-zinc-800">
                    <td className="px-5 py-3 font-medium">{r.ticker}</td>
                    <td className="px-5 py-3">{Math.round(r.mentions)}</td>
                    <td className="px-5 py-3">{r.spike_ratio.toFixed(2)}</td>
                    <td className="px-5 py-3">
                      {(r.bullish_ratio * 100).toFixed(0)}%
                    </td>
                    <td className="px-5 py-3">{r.sentiment_mean.toFixed(3)}</td>
                    <td className="px-5 py-3">
                      {r.conviction_mean.toFixed(3)}
                    </td>
                    <td className="px-5 py-3 text-zinc-300">
                      N:{Math.round(r.news_mentions)} / R:
                      {Math.round(r.reddit_mentions)}
                      {r.cross_source ? (
                        <span className="ml-2 text-emerald-400">✓</span>
                      ) : null}
                    </td>
                  </tr>
                ))}
                {movers.results.length === 0 ? (
                  <tr>
                    <td className="px-5 py-6 text-zinc-400" colSpan={7}>
                      No data yet. Run your local pipeline to populate Neon
                      Postgres.
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>

          <div className="px-5 py-4 text-xs text-zinc-500">
            Tip: keep your local Docker pipeline pointed at Neon
            (DATABASE_URL_SYNC) so this stays fresh.
          </div>
        </div>
      </div>
    </main>
  );
}
