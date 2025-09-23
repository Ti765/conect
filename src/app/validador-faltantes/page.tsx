"use client";

import { useState } from "react";
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
  const { toast } = useToast();

  // --- helpers --------------------------------------------------------------
  function diasEntre(a: string, b: string) {
    if (!a || !b) return 0;
    const d1 = new Date(a + "T00:00:00");
    const d2 = new Date(b + "T00:00:00");
    const diff = Math.abs(d2.getTime() - d1.getTime());
    return Math.floor(diff / (1000 * 60 * 60 * 24));
  }

  async function handleSubmit() {
    // validações simples
    if (!empresa || !dataIni || !dataFim) {
      toast({
        variant: "destructive",
        title: "Preencha código da empresa e o período.",
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
      setProgress(10);

      // corpo mínimo: somente o que o backend precisa
      const body = {
        codi_emp: Number(empresa),
        data_inicio: dataIni,
        data_fim: dataFim,
      };

      const res = await exportReport(body); // POST /api/validador-faltantes/export
      setProgress(70);

      if (!res.ok) {
        let msg = "Falha ao gerar relatório.";
        try {
          const j = await res.json();
          msg = j?.error || msg;
        } catch {
          // ignore
        }
        throw new Error(msg);
      }

      const dispo = res.headers.get("content-disposition") ?? "";
      const match = dispo.match(/filename="?([^"]+)"?/i);
      const filename =
        match?.[1] || `validador_faltantes_${empresa}_${dataIni}_${dataFim}.xlsx`;

      const blob = await res.blob();
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
      toast({
        variant: "destructive",
        title: "Erro ao gerar relatório",
        description: err?.message ?? String(err),
      });
    } finally {
      setRunning(false);
      setTimeout(() => setProgress(0), 1200);
    }
  }

  // --- render ---------------------------------------------------------------
  return (
    <div className="space-y-8">
      <Card className="shadow-lg">
        <CardHeader>
          <CardTitle>Validador de Notas Faltantes</CardTitle>
          <CardDescription>
            Informe o <strong>código da empresa</strong> e o{" "}
            <strong>período</strong> (máx. 3 meses). O backend identifica o CNPJ e
            analisa entradas e saídas automaticamente.
          </CardDescription>
        </CardHeader>

        <CardContent className="space-y-6">
          <div className="grid md:grid-cols-3 gap-4">
            <div>
              <Label>Código da Empresa</Label>
              <Input
                inputMode="numeric"
                placeholder="Ex.: 586"
                value={empresa}
                onChange={(e) => setEmpresa(e.target.value)}
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
            {running ? "Gerando…" : "Gerar Relatório"}
          </Button>

          {progress > 0 && <Progress value={progress} className="h-2" />}
        </CardContent>
      </Card>
    </div>
  );
}
