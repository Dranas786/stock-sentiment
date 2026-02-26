import { NextResponse } from "next/server";
import { getPool } from "@/lib/db";

export async function GET() {
  try {
    const pool = getPool();
    await pool.query("SELECT 1;");
    return NextResponse.json({ status: "ok" });
  } catch (e) {
    return NextResponse.json(
      { status: "error", message: String(e) },
      { status: 500 },
    );
  }
}
