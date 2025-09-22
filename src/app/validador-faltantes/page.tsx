"use client"

import React, { useState } from "react"
import { exportReport } from "./service"
import type { ValidatePayload } from "./types"

export default function ValidadorFaltantesPage() {
  const [payload, setPayload] = useState<Partial<ValidatePayload>>({
    tipo_documento: "NFE",
    direcao: "ENTRADA",
  })
  const [loading, setLoading] = useState(false)

  const handleChange = (k: keyof ValidatePayload, v: any) =>
    setPayload((prev: Partial<ValidatePayload>) => ({ ...prev, [k]: v }))

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setLoading(true)
    try {
      const body = payload as ValidatePayload
      const res = await exportReport(body)
      if (!res.ok) throw new Error("Erro no servidor")
      const blob = await res.blob()
      const url = URL.createObjectURL(blob)
      const a = document.createElement("a")
      a.href = url
      a.download = `relatorio-faltantes.xlsx`
      document.body.appendChild(a)
      a.click()
      a.remove()
      URL.revokeObjectURL(url)
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error(err)
      alert("Falha ao gerar relatório")
    } finally {
      setLoading(false)
    }
  }

  return (
    <main className="p-6">
      <h1 className="text-2xl font-semibold mb-4">Validador de Notas Faltantes</h1>
      <form onSubmit={handleSubmit} className="grid grid-cols-1 gap-3 max-w-xl">
        <label className="flex flex-col">
          Código da empresa
          <input
            type="number"
            required
            onChange={(e) => handleChange("codi_emp", Number(e.target.value))}
            className="border rounded px-2 py-1"
          />
        </label>

        <label className="flex flex-col">
          CNPJ da empresa
          <input
            type="text"
            required
            onChange={(e) => handleChange("cnpj_empresa", e.target.value)}
            className="border rounded px-2 py-1"
          />
        </label>

        <label className="flex flex-col">
          Tipo de documento
          <select
            defaultValue="NFE"
            onChange={(e) => handleChange("tipo_documento", e.target.value)}
            className="border rounded px-2 py-1"
          >
            <option value="NFE">NFE</option>
            <option value="CTE">CTE</option>
          </select>
        </label>

        <label className="flex flex-col">
          Direção
          <select
            defaultValue="ENTRADA"
            onChange={(e) => handleChange("direcao", e.target.value)}
            className="border rounded px-2 py-1"
          >
            <option value="ENTRADA">ENTRADA</option>
            <option value="SAIDA">SAIDA</option>
          </select>
        </label>

        <label className="flex flex-col">
          Data início
          <input
            type="date"
            required
            onChange={(e) => handleChange("data_inicio", e.target.value)}
            className="border rounded px-2 py-1"
          />
        </label>

        <label className="flex flex-col">
          Data fim
          <input
            type="date"
            required
            onChange={(e) => handleChange("data_fim", e.target.value)}
            className="border rounded px-2 py-1"
          />
        </label>

        <div>
          <button
            type="submit"
            disabled={loading}
            className="bg-blue-600 text-white px-4 py-2 rounded disabled:opacity-60"
          >
            {loading ? "Gerando..." : "Gerar relatório"}
          </button>
        </div>
      </form>
    </main>
  )
}
