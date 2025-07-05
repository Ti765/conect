'use client';

import { useState, ChangeEvent } from 'react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Copy } from "lucide-react";
import { useToast } from "@/hooks/use-toast";

export default function CorrectAccessKeyPage() {
  const [inputKey, setInputKey] = useState('');
  const [cleanedKey, setCleanedKey] = useState('');
  const { toast } = useToast();

  const handleInputChange = (event: ChangeEvent<HTMLInputElement>) => {
    const value = event.target.value;
    setInputKey(value);
    setCleanedKey(value.replace(/\D/g, '')); // Remove all non-digit characters
  };

  const copyToClipboard = () => {
    if (cleanedKey) {
      navigator.clipboard.writeText(cleanedKey).then(() => {
        toast({
          title: "Copiado!",
          description: "Chave de acesso copiada para a área de transferência.",
        });
      }).catch(err => {
        toast({
          title: "Erro ao copiar",
          description: "Não foi possível copiar a chave de acesso.",
          variant: "destructive",
        });
      });
    }
  };

  return (
    <main className="flex flex-1 flex-col p-4 md:p-6">
      <div className="flex items-center mb-8">
        <h1 className="font-semibold text-lg md:text-2xl">Corrigir Chave de Acesso</h1>
      </div>
      <Card className="shadow-lg">
        <CardHeader>
          <CardTitle>Limpar Chave de Acesso da NF</CardTitle>
          <CardDescription>
            Cole a chave de acesso no campo abaixo para remover caracteres inválidos.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div>
            <Label htmlFor="accessKeyInput" className="mb-2 block font-semibold">Chave de Acesso (com caracteres extras)</Label>
            <Input
              id="accessKeyInput"
              type="text"
              value={inputKey}
              onChange={handleInputChange}
              placeholder="Cole a chave de acesso aqui (ex: 1234.5678-9012...) "
              className="bg-muted/30 focus:bg-background"
            />
          </div>
          {cleanedKey && (
            <div className="flex items-center space-x-2">
              <div className="flex-1">
                <Label htmlFor="cleanedAccessKey" className="mb-2 block font-semibold">Chave de Acesso (somente números)</Label>
                <Input
                  id="cleanedAccessKey"
                  type="text"
                  value={cleanedKey}
                  readOnly
                  className="bg-green-50 text-green-800 font-mono"
                />
              </div>
              <Button onClick={copyToClipboard} className="self-end">
                <Copy className="mr-2 h-4 w-4" /> Copiar
              </Button>
            </div>
          )}
        </CardContent>
      </Card>
    </main>
  );
}
