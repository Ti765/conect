import type { ValidatePayload } from "./types"

export async function exportReport(payload: ValidatePayload): Promise<Response> {
  return fetch("/api/validador-faltantes/export", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  })
}
