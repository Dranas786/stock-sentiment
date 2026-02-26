export function clampInt(
  value: string | null,
  def: number,
  min: number,
  max: number,
) {
  const n = value ? parseInt(value, 10) : def;
  if (Number.isNaN(n)) return def;
  return Math.max(min, Math.min(max, n));
}

export function clampBool(value: string | null, def: boolean) {
  if (value === null) return def;
  return value === "true" || value === "1";
}
