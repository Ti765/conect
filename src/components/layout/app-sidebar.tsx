
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
import { Button } from "@/components/ui/button";
import { Settings, LogOut } from "lucide-react";

export function AppSidebar() {
  const pathname = usePathname();
  const { open, setOpenMobile } = useSidebar();

  const handleLinkClick = () => {
    if (open && window.innerWidth < 768) { // Close mobile sidebar on link click
        setOpenMobile(false);
    }
  }

  return (
    <Sidebar collapsible="icon" variant="sidebar" side="left" defaultOpen={true}>
      <SidebarHeader className="p-4 border-b">
        <Link href="/" className="flex items-center gap-2" onClick={handleLinkClick}>
          <FiscalFlowLogo className={cn("h-8 w-auto", {"hidden": !open, "group-data-[collapsible=icon]:hidden": open})}/>
          <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className={cn("h-8 w-8 text-primary", {"hidden": open, "group-data-[collapsible=icon]:block": !open})}><path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg>
          <span className={cn("font-semibold text-lg", {"hidden": !open, "group-data-[collapsible=icon]:hidden": open})}>
            {/* Text already in logo, or can be added here if logo is icon only */}
          </span>
        </Link>
      </SidebarHeader>
      <SidebarContent className="p-2">
        <SidebarMenu>
          {navItems.map((item: NavItem) => (
            <SidebarMenuItem key={item.href}>
              <Link href={item.href} passHref legacyBehavior>
                <SidebarMenuButton
                  asChild
                  isActive={pathname === item.href}
                  tooltip={{ children: item.title, className: "group-data-[collapsible=icon]:block hidden"}}
                  onClick={handleLinkClick}
                  className="justify-start"
                >
                  <a>
                    <item.icon className="h-5 w-5" />
                    <span className={cn({"hidden": !open, "group-data-[collapsible=icon]:hidden": open})}>{item.title}</span>
                  </a>
                </SidebarMenuButton>
              </Link>
            </SidebarMenuItem>
          ))}
        </SidebarMenu>
      </SidebarContent>
      <SidebarFooter className="p-2 border-t">
        {/* Placeholder for user actions like settings or logout */}
        <SidebarMenuButton asChild tooltip={{ children: "Settings", className: "group-data-[collapsible=icon]:block hidden"}} className="justify-start">
            <Link href="#">
                <Settings className="h-5 w-5" />
                <span className={cn({"hidden": !open, "group-data-[collapsible=icon]:hidden": open})}>Settings</span>
            </Link>
        </SidebarMenuButton>
        <SidebarMenuButton asChild tooltip={{ children: "Logout", className: "group-data-[collapsible=icon]:block hidden"}} className="justify-start">
            <Link href="#">
                <LogOut className="h-5 w-5" />
                <span className={cn({"hidden": !open, "group-data-[collapsible=icon]:hidden": open})}>Logout</span>
            </Link>
        </SidebarMenuButton>
      </SidebarFooter>
    </Sidebar>
  );
}
