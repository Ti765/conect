
"use server";

import { analyzeFiscalDiscrepancies as analyzeFiscalDiscrepanciesFlow, type AnalyzeFiscalDiscrepanciesInput, type AnalyzeFiscalDiscrepanciesOutput } from "@/ai/flows/analyze-fiscal-discrepancies";
import { z } from "zod";

const FiscalDataSchema = z.object({
  fiscalData: z.string().min(10, "Os dados fiscais devem ter pelo menos 10 caracteres."),
});

export type FormState = {
  message: string;
  fields?: Record<string, string>;
  analysisResult?: AnalyzeFiscalDiscrepanciesOutput;
  success: boolean;
};

export async function analyzeDataAction(
  prevState: FormState,
  formData: FormData
): Promise<FormState> {
  const rawFormData = {
    fiscalData: formData.get("fiscalData") as string,
  };

  const validatedFields = FiscalDataSchema.safeParse(rawFormData);

  if (!validatedFields.success) {
    return {
      message: "Falha na validação. Verifique os campos.",
      fields: validatedFields.error.flatten().fieldErrors as Record<string, string>,
      success: false,
    };
  }

  try {
    const input: AnalyzeFiscalDiscrepanciesInput = {
      fiscalData: validatedFields.data.fiscalData,
    };
    const result = await analyzeFiscalDiscrepanciesFlow(input);
    return {
      message: "Análise concluída com sucesso!",
      analysisResult: result,
      success: true,
    };
  } catch (error) {
    console.error("Error calling AI flow:", error);
    const errorMessage = error instanceof Error ? error.message : "Ocorreu um erro desconhecido durante a análise.";
    return {
      message: `Erro na análise: ${errorMessage}`,
      success: false,
    };
  }
}
