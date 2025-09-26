export type ExportBody = {
  codi_emp: number;
  data_inicio: string; // YYYY-MM-DD
  data_fim: string;    // YYYY-MM-DD
};

/**
 * Chama a rota app router /validador-faltantes/export
 * Retorna o Response bruto (para o caller decidir entre blob/json).
 */
export async function exportReport(
  body: ExportBody,
  signal?: AbortSignal
): Promise<Response> {
  return fetch("/validador-faltantes/export", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    // Importantíssimo: rota retorna binário; não use next.revalidate
    cache: "no-store",
    body: JSON.stringify(body),
    signal,
  });
}
