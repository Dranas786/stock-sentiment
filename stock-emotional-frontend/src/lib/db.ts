import { Pool } from "pg";

declare global {
  var __pgPool: Pool | undefined;
}

/**
 * Reuse a single pool across hot reloads in dev.
 * In serverless prod, Vercel will manage instances; this is still fine for MVP.
 */
export function getPool(): Pool {
  if (!process.env.DATABASE_URL) {
    throw new Error("DATABASE_URL is not set");
  }

  if (!global.__pgPool) {
    global.__pgPool = new Pool({
      connectionString: process.env.DATABASE_URL,
      // Neon uses SSL; sslmode=require in URL is usually enough.
    });
  }

  return global.__pgPool;
}
