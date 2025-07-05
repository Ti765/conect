import { FileText, Package, Truck, BarChart2, Bot, Key, LayoutDashboard } from "lucide-react";

export const navItems = [
  {
    title: "Dashboard", // Added back
    href: "/", // Assuming the dashboard is at the root
    icon: LayoutDashboard, // Added appropriate icon
    label: "Dashboard",
  },
  {
    title: "Classificar Fornecedores",
    href: "/classify-suppliers",
    icon: Package,
    label: "Suppliers",
  },
  {
    title: "Separar CT-e por Regime",
    href: "/separate-cte",
    icon: Truck,
    label: "CT-e",
  },
  {
    title: "Separar XML por Tipo",
    href: "/separate-xml",
    icon: FileText,
    label: "XML",
  },
  {
    title: "Análise de Divergências Fiscais",
    href: "/analyze-discrepancies",
    icon: Bot,
    label: "AI Analyze",
  },
  {
    title: "Corrigir Chave de Acesso",
    href: "/correct-access-key",
    icon: Key,
    label: "Correct Key",
  },
];