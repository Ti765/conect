import { NextResponse } from "next/server"

export async function POST(request: Request) {
  const backend = process.env.FASTAPI_URL || "http://localhost:8000"
  const url = `${backend.replace(/\/$/, "")}/validator/export`
  const body = await request.text()

  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body,
  })

  const headers: Record<string, string> = {}
  res.headers.forEach((v, k) => (headers[k] = v))

  return new NextResponse(res.body, { status: res.status, headers })
}
