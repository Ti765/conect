// C:\projetos\studio\src\app\validador-faltantes\service.ts
export type ExportBody = {
  codi_emp: number;
  data_inicio: string; // "YYYY-MM-DD"
  data_fim: string;    // "YYYY-MM-DD"
};

export async function exportReport(body: ExportBody) {
  return fetch("/validador-faltantes/export", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}
