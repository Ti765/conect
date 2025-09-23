"use client";

import Link from "next/link";
import { SidebarTrigger } from "@/components/ui/sidebar";
import { ThemeToggle } from "@/components/theme-toggle";
import { FiscalFlowLogo } from "@/components/icons/logo";

export function AppHeader() {
  return (
    <header className="sticky top-0 z-40 w-full border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
      <div className="container flex h-16 items-center justify-between px-4 sm:px-6 lg:px-8">
        <div className="flex items-center">
          <div className="md:hidden mr-2">
            <SidebarTrigger />
          </div>

          <Link href="/" className="flex items-center space-x-2" aria-label="Ir para o dashboard">
            <FiscalFlowLogo className="h-8 w-auto" />
            {/* Caso o logo seja apenas ícone, você pode exibir o texto aqui
            <span className="font-semibold text-lg hidden sm:block">FiscalFlow</span> */}
          </Link>
        </div>

        <div className="flex items-center space-x-2">
          <ThemeToggle />
          {/* Placeholder para menu de usuário (quando houver auth) */}
          {/* <UserNav /> */}
        </div>
      </div>
    </header>
  );
}
