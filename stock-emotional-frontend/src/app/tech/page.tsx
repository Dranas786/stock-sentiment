export const dynamic = "force-dynamic";

async function fetchJSON<T>(path: string): Promise<T> {
  const res = await fetch(path, { cache: "no-store" });
  if (!res.ok) throw new Error(`${path} failed: ${res.status}`);
  return res.json();
}

export default async function TechPage() {
  const meta = await fetchJSON<any>("/api/meta");

  const nodes = [
    {
      name: "Collector",
      desc: "Pulls Reddit + NewsAPI, writes raw.posts",
      tech: ["Python", "PRAW", "requests", "kafka-python"],
    },
    {
      name: "Redpanda",
      desc: "Kafka-compatible broker: raw.posts → enriched.posts",
      tech: ["Redpanda / Kafka"],
    },
    {
      name: "Enricher",
      desc: "FinBERT + Emotion + Stance + Ticker extraction",
      tech: ["Transformers", "FinBERT", "RoBERTa Emotion", "BART MNLI"],
    },
    {
      name: "Aggregator",
      desc: "5-min buckets → Postgres aggregates",
      tech: ["Python", "psycopg", "JSONB upserts"],
    },
    {
      name: "Neon Postgres",
      desc: "Stores ticker_agg_5m + history",
      tech: ["Postgres", "Neon"],
    },
    {
      name: "API Routes",
      desc: "Serverless endpoints querying Postgres",
      tech: ["Next.js Route Handlers", "pg"],
    },
    {
      name: "Frontend",
      desc: "Dashboard + Tech page",
      tech: ["Next.js", "Tailwind"],
    },
  ];

  return (
    <main className="min-h-screen bg-zinc-950 text-zinc-100">
      <div className="mx-auto max-w-6xl px-6 py-10">
        <div className="flex items-start justify-between gap-6">
          <div>
            <h1 className="text-3xl font-semibold tracking-tight">
              How it works
            </h1>
            <p className="mt-2 text-zinc-300">
              Recruiter-friendly architecture map powered by live config.
            </p>
          </div>
          <a
            href="/"
            className="text-sm text-zinc-300 underline hover:text-zinc-100"
          >
            Back to dashboard
          </a>
        </div>

        <div className="mt-8 grid gap-4 md:grid-cols-2">
          {nodes.map((n) => (
            <div
              key={n.name}
              className="rounded-xl border border-zinc-800 bg-zinc-900/40 p-5"
            >
              <div className="flex items-center justify-between">
                <h2 className="text-lg font-medium">{n.name}</h2>
              </div>
              <p className="mt-2 text-sm text-zinc-300">{n.desc}</p>
              <div className="mt-3 flex flex-wrap gap-2">
                {n.tech.map((t) => (
                  <span
                    key={t}
                    className="rounded-full border border-zinc-700 bg-zinc-900 px-3 py-1 text-xs text-zinc-200"
                  >
                    {t}
                  </span>
                ))}
              </div>
            </div>
          ))}
        </div>

        <div className="mt-8 rounded-xl border border-zinc-800 bg-zinc-900/40 p-5">
          <h2 className="text-lg font-medium">Live meta</h2>
          <pre className="mt-3 overflow-x-auto rounded-lg bg-zinc-950/60 p-4 text-xs text-zinc-200">
            {JSON.stringify(meta, null, 2)}
          </pre>
        </div>
      </div>
    </main>
  );
}
