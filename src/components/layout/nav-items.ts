import type { LucideIcon } from "lucide-react";
import { FileText, Package, Truck, Key, LayoutDashboard } from "lucide-react";

export type NavItem = {
  title: string;
  href: string;
  icon: LucideIcon;
  label?: string;
};

export const navItems: NavItem[] = [
  {
    title: "Dashboard",
    href: "/",
    icon: LayoutDashboard,
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

  // NOVO: Validador de Notas Faltantes
  {
    title: "Validador de Notas Faltantes",
    href: "/validador-faltantes",
    icon: FileText,
    label: "Validador",
  },

  {
    title: "Corrigir Chave de Acesso",
    href: "/correct-access-key",
    icon: Key,
    label: "Correct Key",
  },
];
