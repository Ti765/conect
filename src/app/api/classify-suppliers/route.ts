// src/app/api/classify-suppliers/route.ts
import { NextRequest, NextResponse } from "next/server";
import { writeFileSync, rmSync, existsSync } from "fs";
import { mkdir } from "fs/promises";
import { join, basename, resolve } from "path";
import { randomUUID } from "crypto";
import { spawn } from "child_process";

export const runtime = "nodejs";

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

    for (const f of fileList) {
      writeFileSync(
        join(inputDir, basename(f.name)),
        Buffer.from(await f.arrayBuffer())
      );
    }

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

    // Verificar se SQLANY_BASE está definido
    const BASE = process.env.SQLANY_BASE;
    if (!BASE) {
      return NextResponse.json(
        { ok: false, error: "SQLANY_BASE não está definido no ambiente" },
        { status: 500 }
      );
    }

    // Usar SQLANY_API_DLL se já estiver definido, caso contrário construir o caminho
    const dllPath = process.env.SQLANY_API_DLL || join(BASE, "lib64", "libdbcapi_r.so");
    
    // Verificar se a biblioteca existe
    if (!existsSync(dllPath)) {
      return NextResponse.json(
        { ok: false, error: `Biblioteca SQL Anywhere não encontrada: ${dllPath}` },
        { status: 500 }
      );
    }

    console.log("[classify-suppliers] Using DBCAPI at:", dllPath);

    // Configurar ambiente para Python
    const env = {
      ...process.env,
      SQLANY_API_DLL: dllPath,
      LD_LIBRARY_PATH: `${BASE}/lib64:${process.env.LD_LIBRARY_PATH ?? ""}`,
      SQLANY_BASE: BASE,
      SQLANY17: BASE, // Algumas versões precisam desta variável
    };

    // Determinar qual Python usar (preferir o do Nix se estiver disponível)
    const venvPy = resolve(process.cwd(), ".venv", "bin", "python3");
    const nixPy = process.env.NIX_PYTHON || "python3"; // Se você definir no Nix
    const PY = process.env.PYTHON_BIN || (existsSync(venvPy) ? venvPy : nixPy);

    console.log("[classify-suppliers] Using Python:", PY);
    console.log("[classify-suppliers] LD_LIBRARY_PATH:", env.LD_LIBRARY_PATH);

    const child = spawn(PY, args, { env });

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (d) => (stdout += d.toString()));
    child.stderr.on("data", (d) => (stderr += d.toString()));

    const code: number = await new Promise((res) => child.on("close", res));

    rmSync(baseTmp, { recursive: true, force: true });

    if (code === 0) {
      return NextResponse.json({ ok: true, log: stdout.trim() });
    } else {
      console.error("[classify-suppliers] python stderr:", stderr);
      return NextResponse.json(
        { ok: false, error: stderr || "Erro desconhecido no script Python" },
        { status: 500 }
      );
    }
  } catch (err: any) {
    rmSync(baseTmp, { recursive: true, force: true });
    console.error("classify-suppliers fatal:", err);
    return NextResponse.json(
      { ok: false, error: err.message || String(err) },
      { status: 500 }
    );
  }
}