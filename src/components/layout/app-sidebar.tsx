"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  Sidebar,
  SidebarHeader,
  SidebarContent,
  SidebarMenu,
  SidebarMenuItem,
  SidebarMenuButton,
  SidebarFooter,
  useSidebar,
} from "@/components/ui/sidebar";
import { FiscalFlowLogo } from "@/components/icons/logo";
import { navItems, type NavItem } from "./nav-items";
import { cn } from "@/lib/utils";
import { Settings, LogOut } from "lucide-react";

export function AppSidebar() {
  const pathname = usePathname();
  const { open, setOpenMobile } = useSidebar();

  const handleLinkClick = () => {
    if (open && typeof window !== "undefined" && window.innerWidth < 768) {
      setOpenMobile(false);
    }
  };

  const isActive = (href: string) => {
    if (!pathname) return false;
    return pathname === href || pathname.startsWith(href + "/");
  };

  return (
    <Sidebar collapsible="icon" variant="sidebar" side="left" defaultOpen>
      <SidebarHeader className="p-4 border-b">
        <Link href="/" className="flex items-center gap-2" onClick={handleLinkClick}>
          <FiscalFlowLogo
            className={cn("h-8 w-auto", {
              hidden: !open,
              "group-data-[collapsible=icon]:hidden": open,
            })}
          />
          {/* Ícone compacto quando colapsado */}
          <svg
            xmlns="http://www.w3.org/2000/svg"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            className={cn("h-8 w-8 text-primary", {
              hidden: open,
              "group-data-[collapsible=icon]:block": !open,
            })}
            aria-hidden="true"
          >
            <path d="M12 2L2 7l10 5 10-5-10-5z" />
            <path d="M2 17l10 5 10-5" />
            <path d="M2 12l10 5 10-5" />
          </svg>

          <span
            className={cn("font-semibold text-lg", {
              hidden: !open,
              "group-data-[collapsible=icon]:hidden": open,
            })}
          />
        </Link>
      </SidebarHeader>

      <SidebarContent className="p-2">
        <SidebarMenu>
          {navItems.map((item: NavItem) => (
            <SidebarMenuItem key={item.href}>
              <SidebarMenuButton
                asChild
                isActive={isActive(item.href)}
                tooltip={{ children: item.title, className: "group-data-[collapsible=icon]:block hidden" }}
                className="justify-start"
              >
                <Link
                  href={item.href}
                  onClick={handleLinkClick}
                  aria-current={isActive(item.href) ? "page" : undefined}
                >
                  <item.icon className="h-5 w-5" />
                  <span
                    className={cn({
                      hidden: !open,
                      "group-data-[collapsible=icon]:hidden": open,
                    })}
                  >
                    {item.title}
                  </span>
                </Link>
              </SidebarMenuButton>
            </SidebarMenuItem>
          ))}
        </SidebarMenu>
      </SidebarContent>

      <SidebarFooter className="p-2 border-t">
        <SidebarMenuButton
          asChild
          tooltip={{ children: "Settings", className: "group-data-[collapsible=icon]:block hidden" }}
          className="justify-start"
        >
          <Link href="#">
            <Settings className="h-5 w-5" />
            <span
              className={cn({
                hidden: !open,
                "group-data-[collapsible=icon]:hidden": open,
              })}
            >
              Settings
            </span>
          </Link>
        </SidebarMenuButton>

        <SidebarMenuButton
          asChild
          tooltip={{ children: "Logout", className: "group-data-[collapsible=icon]:block hidden" }}
          className="justify-start"
        >
          <Link href="#">
            <LogOut className="h-5 w-5" />
            <span
              className={cn({
                hidden: !open,
                "group-data-[collapsible=icon]:hidden": open,
              })}
            >
              Logout
            </span>
          </Link>
        </SidebarMenuButton>
      </SidebarFooter>
    </Sidebar>
  );
}
