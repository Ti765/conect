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
import { classifySuppliers, type ClassifySuppliersForm } from "@/app/classify-suppliers/service";

export default function ClassifySuppliersPage() {
  const [empresa, setEmpresa]   = useState("");
  const [dataIni, setDataIni]   = useState("");
  const [dataFim, setDataFim]   = useState("");
  const [files, setFiles]       = useState<File[]>([]);
  const [running, setRunning]   = useState(false);
  const [progress, setProgress] = useState(0);

  const { toast } = useToast();
  const inputRef  = useRef<HTMLInputElement>(null);

  // ---------------------------------------------------------------
  // Handlers
  // ---------------------------------------------------------------
  function handleFileChange(e: ChangeEvent<HTMLInputElement>) {
    if (e.target.files) setFiles(Array.from(e.target.files));
  }

  function diasEntre(a: string, b: string) {
    if (!a || !b) return 0;
    const d1 = new Date(a + "T00:00:00");
    const d2 = new Date(b + "T00:00:00");
    const diff = Math.abs(d2.getTime() - d1.getTime());
    return Math.floor(diff / (1000 * 60 * 60 * 24));
  }

  async function handleSubmit() {
    if (!empresa || !dataIni || !dataFim || files.length === 0) {
      toast({
        variant: "destructive",
        title: "Preencha todos os campos e selecione a pasta!",
      });
      return;
    }
    if (new Date(dataFim) < new Date(dataIni)) {
      toast({ variant: "destructive", title: "Data fim < data início." });
      return;
    }
    if (diasEntre(dataIni, dataFim) > 92) {
      toast({ variant: "destructive", title: "Período máximo: 3 meses." });
      return;
    }

    try {
      setRunning(true);
      setProgress(10);

      const payload: ClassifySuppliersForm = {
        empresa,
        dataIni,
        dataFim,
        files,
      };

      const res = await classifySuppliers(payload);

      if (!res.ok) {
        let msg = "Falha ao classificar fornecedores.";
        try {
          const j = await res.json();
          msg = j?.error || msg;
        } catch {}
        throw new Error(msg);
      }

      setProgress(80);

      const ctype = res.headers.get("content-type") ?? "";
      if (ctype.includes("application/zip")) {
        // download automático do ZIP
        const blob = await res.blob();
        const dispo = res.headers.get("content-disposition") ?? "";
        const match = dispo.match(/filename="?([^"]+)"?/i);
        const filename =
          match?.[1] || `classificados_${empresa}_${dataIni}_${dataFim}.zip`;

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
        // fallback JSON { ok: true }
        const data = await res.json();
        if (!data.ok) throw new Error(data.error || "Falha desconhecida");
        setProgress(100);
        toast({ title: "Classificação concluída." });
      }
    } catch (err: any) {
      toast({
        variant: "destructive",
        title: "Erro ao classificar",
        description: err?.message ?? String(err),
      });
    } finally {
      setRunning(false);
      setTimeout(() => setProgress(0), 1500);
    }
  }

  // ---------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------
  return (
    <div className="space-y-8">
      <Card>
        <CardHeader>
          <CardTitle>Classificar Fornecedores</CardTitle>
          <CardDescription>
            Selecione a pasta de entradas (ZIP/XML), informe empresa e período,
            depois clique em <strong>Iniciar Classificação</strong>.
          </CardDescription>
        </CardHeader>

        <CardContent className="space-y-6">
          {/* Seletor de pasta */}
          <div>
            <Label>Diretório de Entradas (arraste ou clique)</Label>
            <Input
              ref={inputRef}
              type="file"
              multiple
              onChange={handleFileChange}
              className="cursor-pointer"
              // permite seleção de diretório no Chrome/Edge (usar string p/ evitar warning do React)
              {...({ webkitdirectory: "" } as any)}
            />
            <p className="text-xs text-muted-foreground mt-1">
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
                inputMode="numeric"
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
