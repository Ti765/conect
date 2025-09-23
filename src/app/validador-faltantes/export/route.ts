import { NextRequest, NextResponse } from "next/server";
import { spawn } from "child_process";
import { join } from "path";
import { readFile } from "fs/promises";
import { existsSync } from "fs";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

/** GET de saúde: /validador-faltantes/export */
export async function GET() {
  return NextResponse.json({ ok: true, route: "/validador-faltantes/export" });
}

function pickPython(): { cmd: string; args: string[] } {
  const exe = process.env.PYTHON_EXECUTABLE;
  if (exe) return { cmd: exe, args: [] };
  return { cmd: "python", args: [] };
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

    // Prefixa PATH com Bin64/Bin32 do SQL Anywhere (Windows)
    const base = process.env.SQLANY_BASE || "";
    const bin64 = base ? join(base, "Bin64") : "";
    const bin32 = base ? join(base, "Bin32") : "";
    const PATH = [bin64, bin32, process.env.PATH || ""].filter(Boolean).join(";");

    const env = {
      ...process.env,
      PATH,
      PYTHONUNBUFFERED: "1",
      FISCALFLOW_LOG_LEVEL: process.env.FISCALFLOW_LOG_LEVEL || "DEBUG",
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
    console.log("[validator] PATH prefix:", bin64 || bin32 || "(sem SQLANY_BASE)");

    const child = spawn(cmd, spawnArgs, { env });

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
      return NextResponse.json({ ok: false, error: msg.slice(0, 2000) }, { status: 500 });
    }

    // O app.py imprime: XLSX_OK:<caminho>
    const match = stdout.match(/XLSX_OK:(.+\.xlsx)/i);
    if (!match) {
      console.error("[validator] Saída inesperada do Python; não achei XLSX_OK.");
      return NextResponse.json({ ok: false, error: "Relatório não gerado." }, { status: 500 });
    }

    const filePath = match[1].trim();
    const buf = await readFile(filePath);

    // ✅ Convertemos para Uint8Array (BodyInit aceita ArrayBufferView)
    const u8 = new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);

    return new NextResponse(u8, {
      headers: {
        "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "Content-Disposition": `attachment; filename="validador_faltantes.xlsx"`,
      },
    });
  } catch (err: any) {
    console.error("[validator] Erro geral:", err);
    return NextResponse.json({ ok: false, error: err?.message || String(err) }, { status: 500 });
  }
}
