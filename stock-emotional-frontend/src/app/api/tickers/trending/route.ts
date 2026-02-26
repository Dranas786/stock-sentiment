import { NextResponse } from "next/server";
import { getPool } from "@/lib/db";
import { clampInt } from "@/lib/params";

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);

  const minutes = clampInt(searchParams.get("minutes"), 60, 5, 24 * 60);
  const limit = clampInt(searchParams.get("limit"), 10, 1, 100);

  const pool = getPool();

  const sql = `
    WITH recent AS (
      SELECT
        ticker,
        SUM(mention_count)::int AS mentions,
        SUM(sentiment_sum) AS sentiment_sum,
        SUM(sentiment_count)::int AS sentiment_count
      FROM ticker_agg_5m
      WHERE bucket_start >= NOW() - ($1 || ' minutes')::interval
      GROUP BY ticker
    )
    SELECT
      ticker,
      mentions,
      CASE WHEN sentiment_count > 0 THEN sentiment_sum / sentiment_count ELSE 0 END AS sentiment_mean
    FROM recent
    ORDER BY mentions DESC
    LIMIT $2;
  `;

  const { rows } = await pool.query(sql, [minutes, limit]);

  return NextResponse.json({ minutes, limit, results: rows });
}
