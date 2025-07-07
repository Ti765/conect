// src/app/api/classify-suppliers/route.ts
import { NextRequest, NextResponse } from "next/server";
import { writeFileSync, rmSync, existsSync, readFileSync } from "fs";
import { mkdir } from "fs/promises";
import { join, basename, resolve } from "path";
import { randomUUID } from "crypto";
import { spawn } from "child_process";

export const runtime = "nodejs";

/* Utilitário: pega o primeiro valor preenchido entre vários aliases */
function pick(form: FormData, ...names: string[]): string | null {
  for (const n of names) {
    const v = form.get(n);
    if (v && `${v}`.trim() !== "") return `${v}`;
  }
  return null;
}

export async function POST(req: NextRequest) {
  const jobId   = randomUUID();
  const baseTmp = join(process.cwd(), "tmp", `job_${jobId}`);
  const inputDir = join(baseTmp, "input");
  await mkdir(inputDir, { recursive: true });

  try {
    /* ------------------------------------------------------------------ */
    /* 1. Lê o form e valida parâmetros                                   */
    /* ------------------------------------------------------------------ */
    const form = await req.formData();
    const fileList = [
      ...form.getAll("files"),
      ...(form.get("file") ? [form.get("file") as File] : []),
    ] as File[];

    const empresa = pick(form, "empresa", "company", "codigoEmpresa");
    const dataIni = pick(form, "dataIni", "data_ini", "dataInicial", "startDate");
    const dataFim = pick(form, "dataFim", "data_fim", "dataFinal", "endDate");

    if (!fileList.length || !empresa || !dataIni || !dataFim) {
      return NextResponse.json(
        { ok: false, error: "Parâmetros faltando." },
        { status: 400 }
      );
    }

    /* ------------------------------------------------------------------ */
    /* 2. Salva arquivos enviados no tmp                                  */
    /* ------------------------------------------------------------------ */
    for (const f of fileList) {
      writeFileSync(
        join(inputDir, basename(f.name)),
        Buffer.from(await f.arrayBuffer())
      );
    }

    /* ------------------------------------------------------------------ */
    /* 3. Monta chamada ao script Python                                  */
    /* ------------------------------------------------------------------ */
    const script = join(
      process.cwd(),
      "src",
      "app",
      "classify-suppliers",
      "Classificador_v1.py"
    );

    const args = [
      script,
      "--input-dir", inputDir,
      "--empresa",   empresa,
      "--data-ini",  dataIni,
      "--data-fim",  dataFim,
    ];

    /* ------------------------------------------------------------------ */
    /* 4. Prepara variáveis de ambiente (PATH com Bin64 do SQL Anywhere)   */
    /* ------------------------------------------------------------------ */
    const BASE = process.env.SQLANY_BASE; // ex.: C:\Program Files\SQL Anywhere 17
    if (!BASE) {
      return NextResponse.json(
        { ok: false, error: "SQLANY_BASE não está definido no ambiente" },
        { status: 500 }
      );
    }

    const env = {
      ...process.env,
      SQLANY_BASE: BASE,
      PATH: `${join(BASE, "Bin64")};${process.env.PATH ?? ""}`, // garante driver ODBC
    };

    /* ------------------------------------------------------------------ */
    /* 5. Resolve o executável Python                                     */
    /* ------------------------------------------------------------------ */
    const venvPyWin = resolve(process.cwd(), ".venv", "Scripts", "python.exe");
    const venvPyNix = resolve(process.cwd(), ".venv", "bin", "python3");
    const PY =
      process.env.PYTHON_BIN && existsSync(process.env.PYTHON_BIN)
        ? process.env.PYTHON_BIN
        : existsSync(venvPyWin)
        ? venvPyWin
        : existsSync(venvPyNix)
        ? venvPyNix
        : "python";

    console.log("[classify-suppliers] Using Python :", PY);
    console.log("[classify-suppliers] PATH add     :", join(BASE, "Bin64"));

    /* ------------------------------------------------------------------ */
    /* 6. Executa o script Python                                         */
    /* ------------------------------------------------------------------ */
    const child = spawn(PY, args, { env });

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (d) => (stdout += d.toString()));
    child.stderr.on("data", (d) => (stderr += d.toString()));

    const code: number = await new Promise((res) => child.on("close", res));

    /* ------------------------------------------------------------------ */
    /* 7. Se exit=0, procura ZIP_OK no stdout e devolve o arquivo          */
    /* ------------------------------------------------------------------ */
    if (code === 0) {
      const m = stdout.match(/ZIP_OK:(.+\.zip)/);
      if (!m) {
        console.error("ZIP não encontrado no stdout:", stdout);
        rmSync(baseTmp, { recursive: true, force: true });
        return NextResponse.json({ ok: false, error: "ZIP não gerado" }, { status: 500 });
      }

      const zipPath  = m[1].trim();
      const fileBuf  = readFileSync(zipPath);
      const fileName = basename(zipPath);

      // limpa tmp depois de ler
      rmSync(baseTmp, { recursive: true, force: true });

      return new NextResponse(fileBuf, {
        headers: {
          "Content-Type": "application/zip",
          "Content-Disposition": `attachment; filename=${fileName}`,
        },
      });
    }

    /* ------------------------------------------------------------------ */
    /* 8. Caso erro no script                                             */
    /* ------------------------------------------------------------------ */
    console.error("[classify-suppliers] python stderr:", stderr);
    rmSync(baseTmp, { recursive: true, force: true });
    return NextResponse.json(
      { ok: false, error: stderr || "Erro desconhecido no script Python" },
      { status: 500 }
    );
  } catch (err: any) {
    rmSync(baseTmp, { recursive: true, force: true });
    console.error("[classify-suppliers] fatal:", err);
    return NextResponse.json(
      { ok: false, error: err.message || String(err) },
      { status: 500 }
    );
  }
}
