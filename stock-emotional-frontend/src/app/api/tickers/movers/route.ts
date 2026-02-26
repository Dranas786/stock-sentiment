import { NextResponse } from "next/server";
import { getPool } from "@/lib/db";
import { clampBool, clampInt } from "@/lib/params";

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);

  const windowMinutes = clampInt(
    searchParams.get("window_minutes"),
    60,
    5,
    24 * 60,
  );
  const baselineHours = clampInt(searchParams.get("baseline_hours"), 6, 1, 72);
  const limit = clampInt(searchParams.get("limit"), 10, 1, 100);
  const requireCrossSource = clampBool(
    searchParams.get("require_cross_source"),
    false,
  );

  const pool = getPool();

  const sql = `
    WITH windowed AS (
      SELECT
        ticker,
        SUM(mention_count)::float AS mentions,
        SUM(weighted_mentions)::float AS weighted_mentions,
        SUM(news_count)::float AS news_mentions,
        SUM(reddit_count)::float AS reddit_mentions,
        SUM(sentiment_sum)::float AS sentiment_sum,
        SUM(sentiment_count)::float AS sentiment_count,
        SUM(bullish_count)::float AS bullish,
        SUM(bearish_count)::float AS bearish,
        SUM(neutral_count)::float AS neutral,
        SUM(conviction_sum)::float AS conviction_sum
      FROM ticker_agg_5m
      WHERE bucket_start >= NOW() - ($1 || ' minutes')::interval
      GROUP BY ticker
    ),
    baseline AS (
      SELECT
        ticker,
        (SUM(mention_count)::float / GREATEST($2, 1)) AS baseline_mentions_per_hour
      FROM ticker_agg_5m
      WHERE bucket_start < NOW() - ($1 || ' minutes')::interval
        AND bucket_start >= NOW() - (($1 || ' minutes')::interval + ($2 || ' hours')::interval)
      GROUP BY ticker
    ),
    scored AS (
      SELECT
        w.ticker,
        w.mentions,
        w.news_mentions,
        w.reddit_mentions,
        CASE WHEN w.sentiment_count > 0 THEN w.sentiment_sum / w.sentiment_count ELSE 0 END AS sentiment_mean,
        CASE WHEN (w.bullish + w.bearish) > 0 THEN w.bullish / (w.bullish + w.bearish) ELSE 0 END AS bullish_ratio,
        CASE WHEN w.sentiment_count > 0 THEN w.conviction_sum / w.sentiment_count ELSE 0 END AS conviction_mean,
        COALESCE(b.baseline_mentions_per_hour, 0.0) AS baseline_per_hour,
        ((w.mentions / GREATEST($1::float / 60.0, 0.01)) / (COALESCE(b.baseline_mentions_per_hour, 0.0) + 1.0)) AS spike_ratio,
        ((w.news_mentions > 0 AND w.reddit_mentions > 0)) AS cross_source
      FROM windowed w
      LEFT JOIN baseline b ON b.ticker = w.ticker
    )
    SELECT *
    FROM scored
    WHERE ($3::bool = false OR cross_source = true)
    ORDER BY
      (spike_ratio * 0.45
       + bullish_ratio * 0.25
       + conviction_mean * 0.20
       + sentiment_mean * 0.10) DESC
    LIMIT $4;
  `;

  const { rows } = await pool.query(sql, [
    windowMinutes,
    baselineHours,
    requireCrossSource,
    limit,
  ]);

  return NextResponse.json({
    window_minutes: windowMinutes,
    baseline_hours: baselineHours,
    limit,
    require_cross_source: requireCrossSource,
    results: rows,
  });
}
