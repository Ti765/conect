"use server";

import { z } from "zod";
import JSZip from "jszip";
import { XMLParser } from "fast-xml-parser";

/** Cada arquivo classificado (para exibição e posterior geração do ZIP) */
export type CategorizedFileEntry = {
  name: string;
  /** Conteúdo bruto do XML – necessário para montar o .zip de download */
  content?: string;
};

export type SeparateCteState = {
  message: string;
  success: boolean;
  analysisResult?: {
    simplesNacional: CategorizedFileEntry[];
    regimeNormal: CategorizedFileEntry[];
    valorZerado: CategorizedFileEntry[];
    errors: Array<{ fileName: string; message: string }>;
  };
  /** Erros de validação de formulário */
  fieldErrors?: { cteFiles?: string };
  /** Conteúdo do .zip (base64) devolvido ao frontend */
  zipBase64?: string;
  /** Nome sugerido para o .zip de download */
  zipName?: string;
};

/* -------------------------------------------------------------------------- */
/*                              Validação de arquivos                         */
/* -------------------------------------------------------------------------- */

const FileSchema = z
  .instanceof(File)
  .refine((file) => file.size > 0, "O arquivo não pode estar vazio.")
  .refine(
    (file) =>
      file.type === "application/xml" ||
      file.type === "text/xml" ||
      file.type === "application/zip",
    "Tipo de arquivo inválido. Apenas XML ou ZIP são permitidos."
  );

const FilesSchema = z
  .array(FileSchema)
  .min(1, "Pelo menos um arquivo deve ser enviado.");

/* -------------------------------------------------------------------------- */
/*                           Lógica de classificação                          */
/* -------------------------------------------------------------------------- */

const processXmlContent = (
  xmlContent: string,
  fileName: string,
  categories: NonNullable<SeparateCteState["analysisResult"]>
) => {
  try {
    const parser = new XMLParser({
      removeNSPrefix: true,
      ignoreAttributes: true,
      parseTagValueAsNumber: false,
    });
    const parsedXml = parser.parse(xmlContent);

    // A estrutura pode ser CTe ou cteProc > CTe
    const infCte =
      parsedXml?.cteProc?.CTe?.infCte ||
      parsedXml?.CTe?.infCte ||
      parsedXml?.infCte;

    if (!infCte) {
      categories.errors.push({
        fileName,
        message:
          "Estrutura XML do CT-e inválida ou não reconhecida (infCte não encontrado).",
      });
      return;
    }

    /* ------------------------- Valor total da prestação ------------------------ */
    const vTPrestElement = infCte.vPrest?.vTPrest;
    let vTPrestValue = -1; // Default para algo não‑zero.

    if (vTPrestElement !== undefined && vTPrestElement !== null) {
      vTPrestValue = parseFloat(String(vTPrestElement).trim());
      if (Number.isNaN(vTPrestValue)) {
        categories.errors.push({
          fileName,
          message: `Valor de vTPrest inválido: ${vTPrestElement}`,
        });
        return;
      }
    } else {
      categories.errors.push({
        fileName,
        message: "vTPrest não encontrado no arquivo.",
      });
      return;
    }

    // Valor zerado → categoria própria
    if (vTPrestValue === 0) {
      categories.valorZerado.push({ name: fileName, content: xmlContent });
      return;
    }

    /* ------------------ Classificação por regime tributário ------------------- */
    const crtElement = infCte.emit?.CRT;
    if (crtElement !== undefined && crtElement !== null) {
      const crtValue = String(crtElement).trim();
      if (crtValue === "1") {
        categories.simplesNacional.push({ name: fileName, content: xmlContent });
      } else {
        categories.regimeNormal.push({ name: fileName, content: xmlContent });
      }
    } else {
      categories.errors.push({
        fileName,
        message: "CRT não encontrado no arquivo.",
      });
    }
  } catch (error) {
    categories.errors.push({
      fileName,
      message: `Erro ao processar XML: ${error instanceof Error ? error.message : String(error)}`,
    });
  }
};

/* -------------------------------------------------------------------------- */
/*                               Server Action                                */
/* -------------------------------------------------------------------------- */

export async function separateCteAction(
  _prevState: SeparateCteState,
  formData: FormData
): Promise<SeparateCteState> {
  const files = formData.getAll("cteFiles") as File[];

  const validatedFiles = FilesSchema.safeParse(files);
  if (!validatedFiles.success) {
    const fileErrors = validatedFiles.error.errors
      .map((e) => e.message)
      .join(", ");
    return {
      message: `Erro de validação: ${fileErrors}`,
      success: false,
      fieldErrors: { cteFiles: fileErrors },
    };
  }

  const categories: NonNullable<SeparateCteState["analysisResult"]> = {
    simplesNacional: [],
    regimeNormal: [],
    valorZerado: [],
    errors: [],
  };

  /* -------------------------- Leitura / decomposição ------------------------- */
  for (const file of validatedFiles.data) {
    /* -------------------------------- ZIP ----------------------------------- */
    if (file.type === "application/zip") {
      try {
        const arrayBuffer = await file.arrayBuffer();
        const zip = await JSZip.loadAsync(arrayBuffer);

        const xmlPromises: Promise<{ name: string; content: string }>[] = [];

        for (const entryPath in zip.files) {
          if (
            entryPath.toLowerCase().endsWith(".xml") &&
            !zip.files[entryPath].dir
          ) {
            const zipEntry = zip.files[entryPath];
            xmlPromises.push(
              zipEntry.async("string").then((xmlContent) => ({
                name: `${file.name} → ${zipEntry.name}`,
                content: xmlContent,
              }))
            );
          }
        }

        const xmlFiles = await Promise.all(xmlPromises);
        for (const { name, content } of xmlFiles) {
          processXmlContent(content, name, categories);
        }
      } catch (error) {
        categories.errors.push({
          fileName: file.name,
          message: `Erro ao processar arquivo ZIP: ${
            error instanceof Error ? error.message : String(error)
          }`,
        });
      }
    }
    /* -------------------------------- XML ----------------------------------- */
    else if (
      file.type === "application/xml" ||
      file.type === "text/xml"
    ) {
      try {
        const xmlContent = await file.text();
        processXmlContent(xmlContent, file.name, categories);
      } catch (error) {
        categories.errors.push({
          fileName: file.name,
          message: `Erro ao ler arquivo XML: ${
            error instanceof Error ? error.message : String(error)
          }`,
        });
      }
    } else {
      categories.errors.push({
        fileName: file.name,
        message: `Tipo de arquivo não suportado: ${file.type}`,
      });
    }
  }

  /* ---------------------------- Pós‑processamento ---------------------------- */
  const totalProcessed =
    categories.simplesNacional.length +
    categories.regimeNormal.length +
    categories.valorZerado.length +
    categories.errors.length;

  if (totalProcessed === 0) {
    return {
      message: "Nenhum arquivo XML válido encontrado para processamento.",
      success: false,
      analysisResult: categories,
    };
  }

  /* ----------------------------- Geração do ZIP ----------------------------- */
  let zipBase64: string | undefined;
  let zipName: string | undefined;

  if (
    categories.simplesNacional.length ||
    categories.regimeNormal.length ||
    categories.valorZerado.length
  ) {
    const zip = new JSZip();

    const addFolder = (
      folderName: string,
      files: CategorizedFileEntry[]
    ) => {
      const folder = zip.folder(folderName);
      files.forEach(({ name, content }) => {
        if (content) {
          folder?.file(name, content);
        }
      });
    };

    addFolder("Simples Nacional", categories.simplesNacional);
    addFolder("Regime Normal", categories.regimeNormal);
    addFolder("CT-es com Valor Zerado", categories.valorZerado);

    zipBase64 = await zip.generateAsync({ type: "base64" });
    zipName = `cte_separados_${Math.random().toString(36).slice(2, 10)}.zip`;
  }

  /* -------------------------------- Retorno --------------------------------- */
  return {
    message: "Separação de CT-e concluída.",
    success: true,
    analysisResult: categories,
    zipBase64,
    zipName,
  };
}
