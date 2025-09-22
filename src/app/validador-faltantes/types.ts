export type TipoDocumento = "NFE" | "CTE"
export type Direcao = "ENTRADA" | "SAIDA"

export interface ValidatePayload {
  codi_emp: number
  cnpj_empresa: string
  tipo_documento: TipoDocumento
  direcao: Direcao
  data_inicio: string // ISO date (yyyy-mm-dd)
  data_fim: string // ISO date
}
