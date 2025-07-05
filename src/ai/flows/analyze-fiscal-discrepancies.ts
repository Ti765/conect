// use server'
'use server';

/**
 * @fileOverview This file defines a Genkit flow for analyzing fiscal data discrepancies.
 *
 * - analyzeFiscalDiscrepancies - Analyzes uploaded fiscal data for potential discrepancies, providing explanations and recommendations.
 * - AnalyzeFiscalDiscrepanciesInput - The input type for the analyzeFiscalDiscrepancies function.
 * - AnalyzeFiscalDiscrepanciesOutput - The return type for the analyzeFiscalDiscrepancies function.
 */

import {ai} from '@/ai/genkit';
import {z} from 'genkit';

const AnalyzeFiscalDiscrepanciesInputSchema = z.object({
  fiscalData: z
    .string()
    .describe(
      'The fiscal data to analyze, expected to be in a structured format like JSON or CSV.  The data should contain enough context for the AI to identify discrepancies, such as transaction dates, amounts, tax codes, and counterparty information.'
    ),
});
export type AnalyzeFiscalDiscrepanciesInput = z.infer<typeof AnalyzeFiscalDiscrepanciesInputSchema>;

const AnalyzeFiscalDiscrepanciesOutputSchema = z.object({
  discrepancies: z.array(
    z.object({
      description: z.string().describe('A description of the discrepancy found.'),
      possibleCauses: z.string().describe('Possible causes for the discrepancy.'),
      recommendations: z.string().describe('Recommendations for correcting the discrepancy.'),
    })
  ).describe('A list of discrepancies found in the fiscal data.'),
  summary: z.string().describe('A summary of the analysis, including the number of discrepancies found and their overall impact.'),
});
export type AnalyzeFiscalDiscrepanciesOutput = z.infer<typeof AnalyzeFiscalDiscrepanciesOutputSchema>;

export async function analyzeFiscalDiscrepancies(input: AnalyzeFiscalDiscrepanciesInput): Promise<AnalyzeFiscalDiscrepanciesOutput> {
  return analyzeFiscalDiscrepanciesFlow(input);
}

const analyzeFiscalDiscrepanciesPrompt = ai.definePrompt({
  name: 'analyzeFiscalDiscrepanciesPrompt',
  input: {schema: AnalyzeFiscalDiscrepanciesInputSchema},
  output: {schema: AnalyzeFiscalDiscrepanciesOutputSchema},
  prompt: `You are an expert in fiscal data analysis. Your task is to analyze the provided fiscal data for potential discrepancies and inconsistencies.

  Analyze the following fiscal data:
  {{fiscalData}}

  Identify any discrepancies or inconsistencies, explain the possible causes for each, and provide recommendations for correction. Ensure the output is a valid JSON array of discrepancies, each with a description, possible causes, and recommendations.
  Also provide a summary of the analysis, including the number of discrepancies found and their overall impact.
  `,
});

const analyzeFiscalDiscrepanciesFlow = ai.defineFlow(
  {
    name: 'analyzeFiscalDiscrepanciesFlow',
    inputSchema: AnalyzeFiscalDiscrepanciesInputSchema,
    outputSchema: AnalyzeFiscalDiscrepanciesOutputSchema,
  },
  async input => {
    const {output} = await analyzeFiscalDiscrepanciesPrompt(input);
    return output!;
  }
);
