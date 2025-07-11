"use client";

import { useState, useRef, ChangeEvent } from "react";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Progress } from "@/components/ui/progress";
import { useToast } from "@/hooks/use-toast";

export default function ClassifySuppliersPage() {
  const [empresa, setEmpresa]   = useState("");
  const [dataIni, setDataIni]   = useState("");
  const [dataFim, setDataFim]   = useState("");
  const [files, setFiles]       = useState<File[]>([]);
  const [running, setRunning]   = useState(false);
  const [progress, setProgress] = useState(0);

  const { toast } = useToast();
  const inputRef  = useRef<HTMLInputElement>(null);

  /* --------------------------------------------------------------- */
  /* Handlers                                                        */
  /* --------------------------------------------------------------- */
  function handleFileChange(e: ChangeEvent<HTMLInputElement>) {
    if (e.target.files) setFiles(Array.from(e.target.files));
  }

  async function handleSubmit() {
    if (!empresa || !dataIni || !dataFim || files.length === 0) {
      toast({
        variant: "destructive",
        title: "Preencha todos os campos e selecione a pasta!",
      });
      return;
    }

    /* monta o multipart */
    const form = new FormData();
    form.append("empresa", empresa);
    form.append("dataIni", dataIni);
    form.append("dataFim", dataFim);
    files.forEach((f) =>
      form.append("files", f, (f as any).webkitRelativePath || f.name)
    );

    try {
      setRunning(true);
      setProgress(10);

      const res = await fetch("/api/classify-suppliers", {
        method: "POST",
        body: form,
      });

      if (!res.ok) {
        /* tenta extrair JSON de erro */
        let msg = "Falha desconhecida";
        try {
          const data = await res.json();
          msg = data.error ?? msg;
        } catch (_) {}
        throw new Error(msg);
      }

      /* se OK, pode vir ZIP ou JSON de sucesso */
      const ctype = res.headers.get("content-type") ?? "";

      if (ctype.includes("application/zip")) {
        /* ↓↓↓ inicia download automático ↓↓↓ */
        const blob = await res.blob();
        const dispo = res.headers.get("content-disposition") ?? "";
        const match = dispo.match(/filename="?([^"]+)"?/i);
        const filename = match?.[1] || "classificados.zip";

        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(url);

        setProgress(100);
        toast({ title: "Classificação concluída. Download iniciado." });
      } else {
        /* fallback – espera JSON padrão { ok: true } */
        const data = await res.json();
        if (!data.ok) throw new Error(data.error || "Falha desconhecida");
        setProgress(100);
        toast({ title: "Classificação concluída." });
      }
    } catch (err: any) {
      toast({
        variant: "destructive",
        title: "Erro ao classificar",
        description: err.message,
      });
    } finally {
      setRunning(false);
      setTimeout(() => setProgress(0), 1500);
    }
  }

  /* --------------------------------------------------------------- */
  /* Render                                                          */
  /* --------------------------------------------------------------- */
  return (
    <div className="space-y-8">
      <Card>
        <CardHeader>
          <CardTitle>Classificar Fornecedores</CardTitle>
          <CardDescription>
            Selecione a pasta de entradas, informe empresa e período, depois
            clique em Iniciar.
          </CardDescription>
        </CardHeader>

        <CardContent className="space-y-6">
          {/* Seletor de pasta */}
          <div>
            <Label>Diretório de Entradas (arraste ou clique)</Label>
            <Input
              ref={inputRef}
              type="file"
              webkitdirectory="true"
              multiple
              onChange={handleFileChange}
              className="cursor-pointer"
            />
            <p className="text-xs text-muted-foreground">
              {files.length
                ? `${files.length} arquivo(s) selecionado(s)`
                : "Nenhum arquivo selecionado."}
            </p>
          </div>

          {/* Empresa / Datas */}
          <div className="grid md:grid-cols-3 gap-4">
            <div>
              <Label>Código da Empresa</Label>
              <Input
                value={empresa}
                onChange={(e) => setEmpresa(e.target.value)}
                placeholder="586"
              />
            </div>
            <div>
              <Label>Data Inicial</Label>
              <Input
                type="date"
                value={dataIni}
                onChange={(e) => setDataIni(e.target.value)}
              />
            </div>
            <div>
              <Label>Data Final</Label>
              <Input
                type="date"
                value={dataFim}
                onChange={(e) => setDataFim(e.target.value)}
              />
            </div>
          </div>

          <Button onClick={handleSubmit} disabled={running}>
            {running ? "Processando…" : "Iniciar Classificação"}
          </Button>

          {progress > 0 && <Progress value={progress} className="h-2" />}
        </CardContent>
      </Card>
    </div>
  );
}
