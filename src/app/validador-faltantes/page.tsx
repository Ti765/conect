"use client";

import { useRef, useState } from "react";
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
import { exportReport } from "./service";

export default function ValidadorFaltantesPage() {
  const [empresa, setEmpresa] = useState<string>("");
  const [dataIni, setDataIni] = useState<string>("");
  const [dataFim, setDataFim] = useState<string>("");
  const [running, setRunning] = useState(false);
  const [progress, setProgress] = useState(0);
  const abortRef = useRef<AbortController | null>(null);
  const { toast } = useToast();

  // --- helpers --------------------------------------------------------------
  function diasEntre(a: string, b: string) {
    if (!a || !b) return 0;
    const d1 = new Date(a + "T00:00:00");
    const d2 = new Date(b + "T00:00:00");
    const diff = Math.abs(d2.getTime() - d1.getTime());
    return Math.floor(diff / (1000 * 60 * 60 * 24));
  }

  function resetProgressSoon() {
    setTimeout(() => setProgress(0), 1200);
  }

  async function handleSubmit() {
    if (running) return;

    // validações simples
    if (!empresa || !dataIni || !dataFim) {
      toast({
        variant: "destructive",
        title: "Preencha código da empresa e o período.",
      });
      return;
    }
    if (Number.isNaN(Number(empresa))) {
      toast({
        variant: "destructive",
        title: "Código da empresa deve ser numérico.",
      });
      return;
    }
    if (new Date(dataFim) < new Date(dataIni)) {
      toast({
        variant: "destructive",
        title: "Data fim não pode ser anterior à data início.",
      });
      return;
    }
    if (diasEntre(dataIni, dataFim) > 92) {
      toast({
        variant: "destructive",
        title: "O período máximo é de 3 meses.",
      });
      return;
    }

    try {
      setRunning(true);
      setProgress(8);

      // controlador para cancelar requisição (se o usuário clicar Cancelar)
      const ctl = new AbortController();
      abortRef.current = ctl;

      const body = {
        codi_emp: Number(empresa),
        data_inicio: dataIni,
        data_fim: dataFim,
      } as const;

      setProgress(18);

      // POST /validador-faltantes/export (retorna XLSX binário)
      const res = await exportReport(body, ctl.signal);
      setProgress(65);

      if (!res.ok) {
        // tenta ler JSON de erro da API
        let msg = "Falha ao gerar relatório.";
        try {
          const j = await res.json();
          msg = j?.error || msg;
        } catch {
          // se não for JSON, deixa mensagem padrão
        }
        throw new Error(msg);
      }

      // nome do arquivo via Content-Disposition
      const dispo = res.headers.get("content-disposition") ?? "";
      const match = dispo.match(/filename="?([^"]+)"?/i);
      const filename =
        match?.[1] ||
        `validador_faltantes_${empresa}_${dataIni}_${dataFim}.xlsx`;

      // baixa o binário
      const blob = await res.blob();
      setProgress(85);

      // dispara o download
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);

      setProgress(100);
      toast({ title: "Relatório gerado. Download iniciado." });
    } catch (err: any) {
      if (err?.name === "AbortError") {
        toast({ title: "Operação cancelada." });
      } else {
        toast({
          variant: "destructive",
          title: "Erro ao gerar relatório",
          description: err?.message ?? String(err),
        });
      }
    } finally {
      setRunning(false);
      abortRef.current = null;
      resetProgressSoon();
    }
  }

  function handleCancel() {
    if (abortRef.current) {
      abortRef.current.abort();
      abortRef.current = null;
      setRunning(false);
      setProgress(0);
    }
  }

  // --- render ---------------------------------------------------------------
  return (
    <div className="max-w-4xl mx-auto space-y-8">
      <Card className="shadow-lg">
        <CardHeader>
          <CardTitle>Validador de Notas Faltantes</CardTitle>
          <CardDescription>
            Informe o <strong>código da empresa</strong> e o{" "}
            <strong>período</strong> (máx. 3 meses). O backend identifica o CNPJ e
            analisa entradas/saídas (NFe) e CT-e (tomados) por data de{" "}
            <em>emissão</em>.
          </CardDescription>
        </CardHeader>

        <CardContent className="space-y-6">
          <div className="grid md:grid-cols-3 gap-4">
            <div>
              <Label htmlFor="empresa">Código da Empresa</Label>
              <Input
                id="empresa"
                inputMode="numeric"
                placeholder="Ex.: 586"
                value={empresa}
                onChange={(e) => setEmpresa(e.target.value)}
                disabled={running}
              />
            </div>

            <div>
              <Label htmlFor="dataIni">Data Inicial</Label>
              <Input
                id="dataIni"
                type="date"
                value={dataIni}
                onChange={(e) => setDataIni(e.target.value)}
                disabled={running}
              />
            </div>

            <div>
              <Label htmlFor="dataFim">Data Final</Label>
              <Input
                id="dataFim"
                type="date"
                value={dataFim}
                onChange={(e) => setDataFim(e.target.value)}
                disabled={running}
              />
            </div>
          </div>

          <div className="flex items-center gap-3">
            <Button onClick={handleSubmit} disabled={running}>
              {running ? "Gerando…" : "Gerar Relatório"}
            </Button>
            {running && (
              <Button variant="secondary" onClick={handleCancel}>
                Cancelar
              </Button>
            )}
          </div>

          {progress > 0 && (
            <div className="space-y-2">
              <Progress value={progress} className="h-2" />
              <p className="text-xs text-muted-foreground">
                {progress < 20 && "Preparando…"}
                {progress >= 20 && progress < 70 && "Processando…"}
                {progress >= 70 && progress < 100 && "Finalizando relatório…"}
                {progress === 100 && "Concluído!"}
              </p>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
