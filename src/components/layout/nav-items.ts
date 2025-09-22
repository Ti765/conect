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
  // Removido: Separar XML por Tipo
  // Removido: Análise de Divergências Fiscais
  {
    title: "Corrigir Chave de Acesso",
    href: "/correct-access-key",
    icon: Key,
    label: "Correct Key",
  },
];