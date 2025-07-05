
"use server";

import { z } from "zod";
import JSZip from "jszip";
import { XMLParser } from "fast-xml-parser";

type CategorizedFileEntry = {
  name: string;
  // content?: string; // Might add content later if download is needed
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
  fieldErrors?: { cteFiles?: string };
};

const FileSchema = z.instanceof(File).refine(
  (file) => file.size > 0, "O arquivo não pode estar vazio."
).refine(
  (file) => file.type === "application/xml" || file.type === "text/xml" || file.type === "application/zip",
  "Tipo de arquivo inválido. Apenas XML ou ZIP são permitidos."
);

const FilesSchema = z.array(FileSchema).min(1, "Pelo menos um arquivo deve ser enviado.");


const processXmlContent = (
  xmlContent: string,
  fileName: string,
  categories: {
    simplesNacional: CategorizedFileEntry[];
    regimeNormal: CategorizedFileEntry[];
    valorZerado: CategorizedFileEntry[];
    errors: Array<{ fileName: string; message: string }>;
  }
) => {
  try {
    const parser = new XMLParser({
      removeNSPrefix: true, // Simplifica o acesso removendo prefixos como 'cte:'
      ignoreAttributes: true, // Não precisamos de atributos para esta lógica
      parseTagValueAsNumber: false, // Processaremos números manualmente
    });
    const parsedXml = parser.parse(xmlContent);

    // A estrutura pode ser CTe ou cteProc > CTe
    const infCte = parsedXml?.cteProc?.CTe?.infCte || parsedXml?.CTe?.infCte || parsedXml?.infCte;

    if (!infCte) {
      categories.errors.push({ fileName, message: "Estrutura XML do CT-e inválida ou não reconhecida (infCte não encontrado)." });
      return;
    }
    
    const vTPrestElement = infCte.vPrest?.vTPrest;
    let vTPrestValue = -1; // Default to a value that won't be zero

    if (vTPrestElement !== undefined && vTPrestElement !== null) {
      vTPrestValue = parseFloat(String(vTPrestElement).trim());
      if (isNaN(vTPrestValue)) {
        categories.errors.push({ fileName, message: `Valor de vTPrest inválido: ${vTPrestElement}` });
        return; // Não podemos prosseguir sem um vTPrest válido
      }
    } else {
      categories.errors.push({ fileName, message: "vTPrest não encontrado no arquivo." });
      return; // Não podemos prosseguir sem vTPrest
    }

    if (vTPrestValue === 0) {
      categories.valorZerado.push({ name: fileName });
      return;
    }

    const crtElement = infCte.emit?.CRT;
    if (crtElement !== undefined && crtElement !== null) {
      const crtValue = String(crtElement).trim();
      if (crtValue === "1") {
        categories.simplesNacional.push({ name: fileName });
      } else {
        categories.regimeNormal.push({ name: fileName });
      }
    } else {
      categories.errors.push({ fileName, message: "CRT não encontrado no arquivo." });
    }
  } catch (error) {
    categories.errors.push({ fileName, message: `Erro ao processar XML: ${error instanceof Error ? error.message : String(error)}` });
  }
};

export async function separateCteAction(
  prevState: SeparateCteState,
  formData: FormData
): Promise<SeparateCteState> {
  const files = formData.getAll("cteFiles") as File[];

  const validatedFiles = FilesSchema.safeParse(files);

  if (!validatedFiles.success) {
    const fileErrors = validatedFiles.error.errors.map(e => e.message).join(", ");
    return {
      message: `Erro de validação: ${fileErrors}`,
      success: false,
      fieldErrors: { cteFiles: fileErrors }
    };
  }

  const categories: SeparateCteState["analysisResult"] = {
    simplesNacional: [],
    regimeNormal: [],
    valorZerado: [],
    errors: [],
  };

  for (const file of validatedFiles.data) {
    if (file.type === "application/zip") {
      try {
        const arrayBuffer = await file.arrayBuffer();
        const zip = await JSZip.loadAsync(arrayBuffer);
        const xmlFilePromises = [];

        for (const entryPath in zip.files) {
          if (entryPath.toLowerCase().endsWith(".xml") && !zip.files[entryPath].dir) {
            const zipEntry = zip.files[entryPath];
            xmlFilePromises.push(
              zipEntry.async("string").then(xmlContent => ({
                content: xmlContent,
                name: `${file.name} -> ${zipEntry.name}`,
              }))
            );
          }
        }
        const xmlFilesFromZip = await Promise.all(xmlFilePromises);
        for (const { content, name } of xmlFilesFromZip) {
          processXmlContent(content, name, categories!);
        }
      } catch (error) {
        categories!.errors.push({ fileName: file.name, message: `Erro ao processar arquivo ZIP: ${error instanceof Error ? error.message : String(error)}` });
      }
    } else if (file.type === "application/xml" || file.type === "text/xml") {
      try {
        const xmlContent = await file.text();
        processXmlContent(xmlContent, file.name, categories!);
      } catch (error) {
        categories!.errors.push({ fileName: file.name, message: `Erro ao ler arquivo XML: ${error instanceof Error ? error.message : String(error)}` });
      }
    } else {
        categories!.errors.push({ fileName: file.name, message: `Tipo de arquivo não suportado: ${file.type}` });
    }
  }

  const totalProcessed = categories!.simplesNacional.length + categories!.regimeNormal.length + categories!.valorZerado.length + categories!.errors.length;
  if (totalProcessed === 0 && files.length > 0) {
     return {
        message: "Nenhum arquivo XML válido encontrado para processamento.",
        success: false,
        analysisResult: categories,
     }
  }


  return {
    message: "Separação de CT-e concluída.",
    success: true,
    analysisResult: categories,
  };
}
