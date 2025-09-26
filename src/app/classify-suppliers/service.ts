export interface ClassifySupplierRequest {
  empresa_id: number;
  cnpj?: string;
  periodo_inicio: string; // yyyy-mm-dd
  periodo_fim: string;    // yyyy-mm-dd
}

export interface ClassifySupplierResponse {
  success: boolean;
  fileUrl?: string;
  error?: string;
}

export async function classifySuppliers(
  data: ClassifySupplierRequest
): Promise<Response> {
  return fetch("/api/classify-suppliers", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(data),
  });
}
