import { NextRequest, NextResponse } from "next/server";
import { spawn } from "child_process";
import { join, basename } from "path";
import { readFile } from "fs/promises";
import { existsSync } from "fs";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
// aumenta tolerância em ambientes serverless (ignorado no dev local)
export const maxDuration = 300;

/** GET de saúde: /validador-faltantes/export */
export async function GET() {
  return NextResponse.json({ ok: true, route: "/validador-faltantes/export" });
}

function pickPython(): { cmd: string; args: string[] } {
  const exe = process.env.PYTHON_EXECUTABLE?.trim();
  if (exe) return { cmd: exe, args: [] };

  if (process.platform === "win32") {
    // Preferir o launcher do Windows (py -3) quando disponível
    return { cmd: "py", args: ["-3"] };
  }
  // Unix-like
  return { cmd: "python3", args: [] };
}

function buildEnvPath() {
  const base = process.env.SQLANY_BASE || "";
  const sep = process.platform === "win32" ? ";" : ":";

  const candidates = base
    ? [
        join(base, "Bin64"),
        join(base, "Bin32"),
        join(base, "bin64"),
        join(base, "bin32"),
      ]
    : [];

  const current = process.env.PATH || process.env.Path || "";
  // mantém apenas diretórios existentes
  const existing = candidates.filter((p) => {
    try {
      return !!p && existsSync(p);
    } catch {
      return false;
    }
  });

  // prefixa os diretórios do SQL Anywhere ao PATH atual
  return [ ...existing, current ].filter(Boolean).join(sep);
}

export async function POST(req: NextRequest) {
  try {
    const data = await req.json().catch(() => null);
    if (!data) {
      return NextResponse.json({ ok: false, error: "JSON inválido." }, { status: 400 });
    }

    const { codi_emp, data_inicio, data_fim } = data as {
      codi_emp?: number;
      data_inicio?: string;
      data_fim?: string;
    };
    if (!codi_emp || !data_inicio || !data_fim) {
      return NextResponse.json(
        { ok: false, error: "Parâmetros obrigatórios: codi_emp, data_inicio, data_fim." },
        { status: 400 }
      );
    }

    const scriptPath = join(process.cwd(), "src", "app", "validador-faltantes", "app.py");
    if (!existsSync(scriptPath)) {
      console.error("[validator] app.py não encontrado:", scriptPath);
      return NextResponse.json({ ok: false, error: `Script não encontrado em ${scriptPath}` }, { status: 500 });
    }

    // PATH reforçado com diretórios do SQL Anywhere (cross-plataforma)
    const PATH = buildEnvPath();

    const env = {
      ...process.env,
      PATH,
      PYTHONUNBUFFERED: "1",
      // mais verboso por padrão para investigarmos eventuais problemas no servidor
      FISCALFLOW_LOG_LEVEL: process.env.FISCALFLOW_LOG_LEVEL || "INFO",
    };

    const { cmd, args } = pickPython();
    const spawnArgs = [
      ...args,
      scriptPath,
      "--empresa",
      String(codi_emp),
      "--data-ini",
      String(data_inicio),
      "--data-fim",
      String(data_fim),
    ];

    console.log("[validator] EXEC:", cmd, spawnArgs.join(" "));
    if (process.env.SQLANY_BASE) {
      console.log("[validator] SQLANY_BASE:", process.env.SQLANY_BASE);
    }
    console.log("[validator] PATH (prefixado):", (PATH || "").split(process.platform === "win32" ? ";" : ":")[0] || "(vazio)");

    // Em Windows, evita piscar janela do console
    const child = spawn(cmd, spawnArgs, { env, windowsHide: true });

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (d: Buffer) => {
      const s = d.toString();
      stdout += s;
      s.split(/\r?\n/).forEach((line: string) => line && console.log("[python]", line));
    });

    child.stderr.on("data", (d: Buffer) => {
      const s = d.toString();
      stderr += s;
      s.split(/\r?\n/).forEach((line: string) => line && console.error("[python:err]", line));
    });

    const exitCode: number = await new Promise((res) => child.on("close", res));
    if (exitCode !== 0) {
      const msg = stderr || stdout || "Falha desconhecida no processamento.";
      return NextResponse.json({ ok: false, error: msg.slice(0, 4000) }, { status: 500 });
    }

    // O app.py imprime: XLSX_OK:<caminho completo do arquivo>
    // Regex mais tolerante (não captura quebras de linha e aceita barras invertidas)
    const match = stdout.match(/XLSX_OK:([^\r\n]+\.xlsx)/i);
    if (!match) {
      console.error("[validator] Saída inesperada do Python; não achei XLSX_OK.");
      return NextResponse.json({ ok: false, error: "Relatório não gerado (XLSX_OK não encontrado)." }, { status: 500 });
    }

    const filePath = match[1].trim().replace(/^"+|"+$/g, "");
    const buf = await readFile(filePath);
    const u8 = new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);

    const fname = basename(filePath) || "validador_faltantes.xlsx";

    return new NextResponse(u8, {
      headers: {
        "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "Content-Disposition": `attachment; filename="${fname}"`,
        "Cache-Control": "no-store",
        "Content-Length": String(u8.byteLength),
      },
    });
  } catch (err: any) {
    console.error("[validator] Erro geral:", err);
    return NextResponse.json({ ok: false, error: err?.message || String(err) }, { status: 500 });
  }
}
