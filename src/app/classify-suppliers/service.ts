// Envia multipart/form-data com a pasta/arquivos e parâmetros
export type ClassifySuppliersForm = {
  empresa: string;
  dataIni: string; // yyyy-mm-dd
  dataFim: string; // yyyy-mm-dd
  files: File[];
};

export async function classifySuppliers(form: ClassifySuppliersForm): Promise<Response> {
  const fd = new FormData();
  fd.append("empresa", form.empresa);
  fd.append("dataIni", form.dataIni);
  fd.append("dataFim", form.dataFim);
  for (const f of form.files) {
    // mantém webkitRelativePath se vier de um diretório
    const name = (f as any).webkitRelativePath || f.name;
    fd.append("files", f, name);
  }

  return fetch("/api/classify-suppliers", {
    method: "POST",
    body: fd,
  });
}
