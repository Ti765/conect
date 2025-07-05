
"use client";

import { useState, useEffect, useActionState } from "react";
import { useFormState, useFormStatus } from "react-dom";
import { BrainCircuit, AlertTriangle, CheckCircle, Info, Lightbulb, FileText, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Textarea } from "@/components/ui/textarea";
import { Label } from "@/components/ui/label";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from "@/components/ui/accordion";
import type { AnalyzeFiscalDiscrepanciesOutput } from "@/ai/flows/analyze-fiscal-discrepancies";
import { analyzeDataAction, type FormState } from "./actions";
import { ScrollArea } from "@/components/ui/scroll-area";

const initialState: FormState = {
  message: "",
  success: false,
};

function SubmitButton() {
  const { pending } = useFormStatus();
  return (
    <Button type="submit" disabled={pending} size="lg" className="w-full sm:w-auto bg-gradient-to-r from-primary via-blue-600 to-purple-600 hover:opacity-90 text-primary-foreground">
      {pending ? (
        <>
          <Loader2 className="mr-2 h-5 w-5 animate-spin" />
          Analisando...
        </>
      ) : (
        <>
          <BrainCircuit className="mr-2 h-5 w-5" />
          Analisar Dados Fiscais
        </>
      )}
    </Button>
  );
}

export default function AnalyzeDiscrepanciesPage() {
  const [state, formAction] = useActionState(analyzeDataAction, initialState);
  const [analysisResult, setAnalysisResult] = useState<AnalyzeFiscalDiscrepanciesOutput | undefined>(undefined);
  const [formKey, setFormKey] = useState(Date.now()); // To reset form

  useEffect(() => {
    if (state.success && state.analysisResult) {
      setAnalysisResult(state.analysisResult);
    } else if (!state.success && state.message) {
       setAnalysisResult(undefined); // Clear previous results on error
    }
    // Do not reset form on every state change, only on successful submission or explicit reset
  }, [state]);
  
  const handleReset = () => {
    setAnalysisResult(undefined);
    setFormKey(Date.now()); // This will recreate the form, effectively resetting it
    // Note: useFormState doesn't have a built-in reset, so re-keying the form is a common pattern.
    // Or manually reset form fields if using react-hook-form with useForm.
    // For now, this page uses useFormState directly.
  };


  return (
    <div className="space-y-8">
      <header className="flex items-center space-x-3">
        <BrainCircuit className="h-8 w-8 text-primary" />
        <div>
          <h1 className="text-3xl font-bold">Análise de Divergências Fiscais com IA</h1>
          <p className="text-muted-foreground">
            Utilize inteligência artificial para identificar potenciais divergências, entender causas e receber recomendações.
          </p>
        </div>
      </header>

      <Card className="shadow-lg">
        <form action={formAction} key={formKey}>
          <CardHeader>
            <CardTitle>Fornecer Dados Fiscais</CardTitle>
            <CardDescription>
              Cole os dados fiscais (ex: CSV, JSON, ou texto estruturado) no campo abaixo para análise.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <Label htmlFor="fiscalData" className="mb-2 block font-semibold">Dados Fiscais</Label>
              <Textarea
                id="fiscalData"
                name="fiscalData"
                rows={10}
                placeholder="Exemplo: Data,Valor,CNPJ,TipoNota...\n2023-01-15,150.00,00.000.000/0001-00,Entrada..."
                className="min-h-[200px] bg-muted/30 focus:bg-background"
                aria-describedby="fiscalData-error"
              />
              {state.fields?.fiscalData && <p id="fiscalData-error" className="text-sm text-destructive mt-1">{state.fields.fiscalData}</p>}
            </div>
            <Alert>
              <Info className="h-4 w-4" />
              <AlertTitle>Dica!</AlertTitle>
              <AlertDescription>
                Forneça dados bem estruturados e com contexto (datas, valores, códigos, etc.) para uma análise mais precisa.
              </AlertDescription>
            </Alert>
          </CardContent>
          <CardFooter className="flex flex-col sm:flex-row justify-between items-center gap-4 border-t pt-6">
            <SubmitButton />
            {analysisResult && (
                 <Button type="button" variant="outline" onClick={handleReset}>
                    Limpar e Nova Análise
                </Button>
            )}
          </CardFooter>
        </form>
      </Card>

      {state.message && !state.success && (
        <Alert variant={state.success ? "default" : "destructive"} className="mt-4">
          {state.success ? <CheckCircle className="h-4 w-4" /> : <AlertTriangle className="h-4 w-4" />}
          <AlertTitle>{state.success ? "Sucesso" : "Erro na Análise"}</AlertTitle>
          <AlertDescription>{state.message}</AlertDescription>
        </Alert>
      )}

      {analysisResult && (
        <Card className="shadow-lg mt-8">
          <CardHeader>
            <CardTitle className="text-2xl flex items-center">
              <FileText className="mr-2 h-6 w-6 text-primary" />
              Resultados da Análise de IA
            </CardTitle>
            <CardDescription>{analysisResult.summary}</CardDescription>
          </CardHeader>
          <CardContent>
            {analysisResult.discrepancies.length > 0 ? (
              <Accordion type="single" collapsible className="w-full">
                {analysisResult.discrepancies.map((discrepancy, index) => (
                  <AccordionItem value={`item-${index}`} key={index}>
                    <AccordionTrigger className="hover:no-underline">
                      <div className="flex items-center text-left">
                        <AlertTriangle className="h-5 w-5 mr-3 text-destructive flex-shrink-0" />
                        <span className="font-semibold">{`Divergência ${index + 1}: ${discrepancy.description.substring(0,80)}...`}</span>
                      </div>
                    </AccordionTrigger>
                    <AccordionContent className="space-y-4 p-4 bg-muted/30 rounded-md">
                       <div>
                        <h4 className="font-semibold text-foreground mb-1">Descrição Completa:</h4>
                        <ScrollArea className="h-20 w-full rounded-md border p-2 text-sm">
                           <p>{discrepancy.description}</p>
                        </ScrollArea>
                      </div>
                      <div>
                        <h4 className="font-semibold text-foreground mb-1 flex items-center">
                          <Info className="h-4 w-4 mr-2 text-blue-500" />
                          Causas Possíveis:
                        </h4>
                         <ScrollArea className="h-20 w-full rounded-md border p-2 text-sm">
                            <p>{discrepancy.possibleCauses}</p>
                         </ScrollArea>
                      </div>
                      <div>
                        <h4 className="font-semibold text-foreground mb-1 flex items-center">
                          <Lightbulb className="h-4 w-4 mr-2 text-yellow-500" />
                          Recomendações:
                        </h4>
                        <ScrollArea className="h-20 w-full rounded-md border p-2 text-sm">
                            <p>{discrepancy.recommendations}</p>
                        </ScrollArea>
                      </div>
                    </AccordionContent>
                  </AccordionItem>
                ))}
              </Accordion>
            ) : (
              <Alert className="bg-green-500/10 border-green-500/50 text-green-700 dark:text-green-400">
                <CheckCircle className="h-5 w-5 text-green-600 dark:text-green-500" />
                <AlertTitle className="font-semibold">Nenhuma Divergência Encontrada</AlertTitle>
                <AlertDescription>
                  A análise foi concluída e não foram identificadas divergências nos dados fornecidos.
                </AlertDescription>
              </Alert>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
