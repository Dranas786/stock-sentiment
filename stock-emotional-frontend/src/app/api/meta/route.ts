import { NextResponse } from "next/server";

export async function GET() {
  return NextResponse.json({
    ui: "Next.js + Tailwind",
    api: "Next.js Route Handlers (serverless)",
    db: "Neon Postgres",
    pipeline: [
      "collector (Reddit + NewsAPI) -> raw.posts",
      "enricher (FinBERT + Emotion + Stance) -> enriched.posts",
      "aggregator (5m buckets) -> ticker_agg_5m",
      "api routes -> frontend",
    ],
    topics: { raw: "raw.posts", enriched: "enriched.posts" },
    models: {
      sentiment: "ProsusAI/finbert",
      emotion: "cardiffnlp/twitter-roberta-base-emotion",
      stance: "facebook/bart-large-mnli",
    },
    aggregation: { bucket_minutes: 5, table: "ticker_agg_5m" },
  });
}
