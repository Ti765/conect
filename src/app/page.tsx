
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { ArrowRight, BarChart3, FileText, AlertTriangle } from "lucide-react";
import Image from "next/image";
import Link from "next/link";

export default function DashboardPage() {
  const indicators = [
    { title: "Total de Notas Emitidas", value: "1,250", icon: FileText, trend: "+5% vs mês anterior", dataAiHint: "document data" },
    { title: "Valor Total Transacionado", value: "R$ 345.670,00", icon: BarChart3, trend: "+2.3% vs mês anterior", dataAiHint: "finance chart" },
    { title: "Divergências Encontradas", value: "12", icon: AlertTriangle, trend: "-3 vs semana anterior", color: "text-destructive", dataAiHint: "alert warning" },
    { title: "Fornecedores Ativos", value: "87", icon: UsersIcon, trend: "+2 novos", dataAiHint: "team collaboration" },
  ];

  return (
    <div className="space-y-8">
      <section className="bg-card border border-border rounded-xl p-8 shadow-lg">
        <div className="grid md:grid-cols-2 gap-8 items-center">
          <div>
            <h1 className="text-4xl font-bold tracking-tight bg-gradient-to-r from-red-500 via-primary to-accent bg-clip-text text-transparent mb-4">
              Bem-vindo ao FiscalFlow
            </h1>
            <p className="text-lg text-muted-foreground mb-6">
              Sua plataforma inteligente para automação e análise fiscal. Simplifique processos, ganhe insights e tome decisões mais assertivas.
            </p>
            <Link href="/analyze-discrepancies">
              <Button size="lg" className="bg-gradient-to-r from-primary via-blue-600 to-purple-600 hover:opacity-90 text-primary-foreground">
                Analisar Divergências com IA
                <ArrowRight className="ml-2 h-5 w-5" />
              </Button>
            </Link>
          </div>
          <div>
            <Image
              src="https://placehold.co/600x400.png"
              alt="Fiscal Analytics Illustration"
              width={600}
              height={400}
              className="rounded-lg shadow-md"
              data-ai-hint="data analytics"
              priority // Added priority to prevent lazy loading for LCP image
            />
          </div>
        </div>
      </section>

      <section>
        <h2 className="text-2xl font-semibold mb-6">Principais Indicadores Fiscais</h2>
        <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-4">
          {indicators.map((indicator, index) => (
            <Card key={index} className="shadow-md hover:shadow-lg transition-shadow">
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">{indicator.title}</CardTitle>
                <indicator.icon className={`h-5 w-5 ${indicator.color || 'text-muted-foreground'}`} />
              </CardHeader>
              <CardContent>
                <div className={`text-2xl font-bold ${indicator.color || ''}`}>{indicator.value}</div>
                <p className="text-xs text-muted-foreground">{indicator.trend}</p>
              </CardContent>
            </Card>
          ))}
        </div>
      </section>

      <section>
        <h2 className="text-2xl font-semibold mb-6">Visualizações Avançadas (Em Breve)</h2>
        <Card className="shadow-md">
          <CardHeader>
            <CardTitle>Widgets 3D e Dashboards Personalizados</CardTitle>
            <CardDescription>
              Em breve, explore seus dados fiscais com visualizações interativas em 3D e crie dashboards personalizados para acompanhar os KPIs mais importantes para o seu negócio.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="flex items-center justify-center h-40 bg-muted/50 rounded-md">
              <p className="text-muted-foreground">Placeholder para visualizações 3D</p>
            </div>
          </CardContent>
        </Card>
      </section>
    </div>
  );
}

// Custom UsersIcon as it might not be available in lucide-react or for specific styling
function UsersIcon(props: React.SVGProps<SVGSVGElement>) {
  return (
    <svg
      {...props}
      xmlns="http://www.w3.org/2000/svg"
      width="24"
      height="24"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2" />
      <circle cx="9" cy="7" r="4" />
      <path d="M22 21v-2a4 4 0 0 0-3-3.87" />
      <path d="M16 3.13a4 4 0 0 1 0 7.75" />
    </svg>
  );
}
