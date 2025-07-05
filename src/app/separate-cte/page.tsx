
"use client";

import { useActionState, useEffect, useState } from "react";
import { useFormStatus } from "react-dom";
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { UploadCloud, Split, Loader2, AlertTriangle, CheckCircle, FileText, FolderOpen, XCircle } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from "@/components/ui/accordion";
import { ScrollArea } from "@/components/ui/scroll-area";
import { separateCteAction, type SeparateCteState } from "./actions";

const initialState: SeparateCteState = {
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
          Processando...
        </>
      ) : (
        <>
          <UploadCloud className="mr-2 h-5 w-5" />
          Iniciar Separação de CT-e
        </>
      )}
    </Button>
  );
}

export default function SeparateCtePage() {
  const [state, formAction] = useActionState(separateCteAction, initialState);
  const [formKey, setFormKey] = useState(Date.now()); // To reset form, including file input

  const handleReset = () => {
    setFormKey(Date.now()); 
    // The useActionState doesn't have a direct reset for its internal state easily from here
    // but re-keying the form resets file inputs and visual state tied to 'state' will update on next action.
    // For a full reset of 'state' itself, you might need to manage it more complexly or accept
    // that the message/success from previous run might briefly show if not cleared by a new action.
    // A common pattern is to also call a function that resets the state a parent might hold,
    // or use a context, or simply rely on the new formAction to overwrite it.
  };
  
  useEffect(() => {
    if (state.message && !state.success) {
      // Optionally, show a toast for errors if preferred
      // toast({ variant: "destructive", title: "Erro", description: state.message });
    }
  }, [state]);


  return (
    <div className="space-y-8">
      <header className="flex items-center space-x-3">
        <Split className="h-8 w-8 text-primary" />
        <div>
          <h1 className="text-3xl font-bold">Separar CT-e por Regime Tributário</h1>
          <p className="text-muted-foreground">
            Importe arquivos CT-e (XML ou ZIP contendo XMLs) para separá-los com base no regime tributário (CRT) e valor.
          </p>
        </div>
      </header>

      <Card className="shadow-lg">
        <form action={formAction} key={formKey}>
          <CardHeader>
            <CardTitle>Importar Arquivos CT-e</CardTitle>
            <CardDescription>
              Selecione ou arraste os arquivos CT-e (XML ou ZIP). Múltiplos arquivos são permitidos.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-6">
            <div className="grid w-full max-w-lg items-center gap-2">
              <Label htmlFor="cte-files" className="sr-only">Arquivos CT-e</Label>
              <Input 
                id="cte-files" 
                name="cteFiles" 
                type="file" 
                multiple 
                className="cursor-pointer file:text-primary file:font-semibold hover:file:bg-primary/10"
                accept=".xml,.zip"
                aria-describedby="cte-files-error"
              />
              {state.fieldErrors?.cteFiles && <p id="cte-files-error" className="text-sm text-destructive mt-1">{state.fieldErrors.cteFiles}</p>}
            </div>
            
            {/* Basic drag and drop visual cue, actual drop handling not implemented in this iteration */}
            <div className="border-2 border-dashed border-border rounded-lg p-12 flex flex-col items-center justify-center text-center bg-muted/20 hover:border-primary transition-colors">
              <UploadCloud className="h-12 w-12 text-muted-foreground mb-4" />
              <p className="text-lg font-semibold text-foreground mb-1">Arraste e solte os arquivos aqui</p>
              <p className="text-sm text-muted-foreground">ou clique no botão acima para selecionar</p>
            </div>
          </CardContent>
          <CardFooter className="flex flex-col sm:flex-row justify-between items-center gap-4 border-t pt-6">
            <SubmitButton />
            {(state.success || state.analysisResult) && (
                 <Button type="button" variant="outline" onClick={handleReset}>
                    Limpar e Nova Separação
                </Button>
            )}
          </CardFooter>
        </form>
      </Card>

      {state.message && (state.success === false) && (
        <Alert variant="destructive" className="mt-4">
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Erro na Separação</AlertTitle>
          <AlertDescription>{state.message}</AlertDescription>
        </Alert>
      )}
      
      {state.analysisResult && (
        <Card className="shadow-lg mt-8">
          <CardHeader>
            <CardTitle className="text-2xl flex items-center">
              <FileText className="mr-2 h-6 w-6 text-primary" />
              Resultados da Separação
            </CardTitle>
            <CardDescription>
              {state.success ? state.message : "Processamento concluído com os seguintes resultados e/ou erros."}
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <Accordion type="multiple" defaultValue={["simples", "normal", "zerado", "erros"]}>
              {state.analysisResult.simplesNacional.length > 0 && (
                <AccordionItem value="simples">
                  <AccordionTrigger>
                    <div className="flex items-center">
                      <FolderOpen className="mr-2 h-5 w-5 text-green-500" />
                      Simples Nacional ({state.analysisResult.simplesNacional.length})
                    </div>
                  </AccordionTrigger>
                  <AccordionContent>
                    <ScrollArea className="h-40 w-full rounded-md border p-2">
                      <ul className="list-disc pl-5 text-sm">
                        {state.analysisResult.simplesNacional.map((file, idx) => <li key={`sn-${idx}`}>{file.name}</li>)}
                      </ul>
                    </ScrollArea>
                  </AccordionContent>
                </AccordionItem>
              )}
              {state.analysisResult.regimeNormal.length > 0 && (
                <AccordionItem value="normal">
                  <AccordionTrigger>
                    <div className="flex items-center">
                      <FolderOpen className="mr-2 h-5 w-5 text-blue-500" />
                      Regime Normal ({state.analysisResult.regimeNormal.length})
                    </div>
                  </AccordionTrigger>
                  <AccordionContent>
                    <ScrollArea className="h-40 w-full rounded-md border p-2">
                      <ul className="list-disc pl-5 text-sm">
                        {state.analysisResult.regimeNormal.map((file, idx) => <li key={`rn-${idx}`}>{file.name}</li>)}
                      </ul>
                    </ScrollArea>
                  </AccordionContent>
                </AccordionItem>
              )}
              {state.analysisResult.valorZerado.length > 0 && (
                <AccordionItem value="zerado">
                  <AccordionTrigger>
                    <div className="flex items-center">
                      <FolderOpen className="mr-2 h-5 w-5 text-yellow-500" />
                      CT-es com Valor Zerado ({state.analysisResult.valorZerado.length})
                    </div>
                  </AccordionTrigger>
                  <AccordionContent>
                    <ScrollArea className="h-40 w-full rounded-md border p-2">
                      <ul className="list-disc pl-5 text-sm">
                        {state.analysisResult.valorZerado.map((file, idx) => <li key={`vz-${idx}`}>{file.name}</li>)}
                      </ul>
                    </ScrollArea>
                  </AccordionContent>
                </AccordionItem>
              )}
            </Accordion>

            {state.analysisResult.errors.length > 0 && (
              <Alert variant="destructive" className="mt-4">
                <XCircle className="h-5 w-5" />
                <AlertTitle>Erros no Processamento</AlertTitle>
                <AlertDescription>
                  <ScrollArea className="h-40 w-full rounded-md p-2">
                    <ul className="list-disc pl-5 text-sm space-y-1">
                      {state.analysisResult.errors.map((err, idx) => (
                        <li key={`err-${idx}`}><strong>{err.fileName}:</strong> {err.message}</li>
                      ))}
                    </ul>
                  </ScrollArea>
                </AlertDescription>
              </Alert>
            )}

            {state.analysisResult.simplesNacional.length === 0 &&
             state.analysisResult.regimeNormal.length === 0 &&
             state.analysisResult.valorZerado.length === 0 &&
             state.analysisResult.errors.length === 0 && (
              <Alert className="bg-green-500/10 border-green-500/50 text-green-700 dark:text-green-400">
                <CheckCircle className="h-5 w-5 text-green-600 dark:text-green-500" />
                <AlertTitle className="font-semibold">Nenhum arquivo processado ou encontrado</AlertTitle>
                <AlertDescription>
                  Verifique os arquivos enviados. Nenhum CT-e foi classificado ou encontrado nos arquivos fornecidos.
                </AlertDescription>
              </Alert>
            )}

          </CardContent>
        </Card>
      )}
    </div>
  );
}
