
"use client";

import Link from "next/link";
import { SidebarTrigger } from "@/components/ui/sidebar";
import { ThemeToggle } from "@/components/theme-toggle";
import { FiscalFlowLogo } from "@/components/icons/logo";
import { Button } from "@/components/ui/button";

export function AppHeader() {
  return (
    <header className="sticky top-0 z-40 w-full border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
      <div className="container flex h-16 items-center justify-between px-4 sm:px-6 lg:px-8">
        <div className="flex items-center">
          <div className="md:hidden mr-2">
            <SidebarTrigger />
          </div>
          <Link href="/" className="flex items-center space-x-2">
            <FiscalFlowLogo className="h-8 w-auto" />
          </Link>
        </div>
        
        <div className="flex items-center space-x-2">
          <ThemeToggle />
           {/* Placeholder for User Profile Dropdown if needed later */}
           {/* <UserNav /> */}
        </div>
      </div>
    </header>
  );
}

// Placeholder for UserNav if you add authentication
// function UserNav() {
//   return (
//     <DropdownMenu>
//       <DropdownMenuTrigger asChild>
//         <Button variant="ghost" className="relative h-8 w-8 rounded-full">
//           <Avatar className="h-8 w-8">
//             <AvatarImage src="https://placehold.co/40x40.png" alt="@shadcn" />
//             <AvatarFallback>SC</AvatarFallback>
//           </Avatar>
//         </Button>
//       </DropdownMenuTrigger>
//       <DropdownMenuContent className="w-56" align="end" forceMount>
//         <DropdownMenuLabel className="font-normal">
//           <div className="flex flex-col space-y-1">
//             <p className="text-sm font-medium leading-none">shadcn</p>
//             <p className="text-xs leading-none text-muted-foreground">
//               m@example.com
//             </p>
//           </div>
//         </DropdownMenuLabel>
//         <DropdownMenuSeparator />
//         <DropdownMenuGroup>
//           <DropdownMenuItem>Profile</DropdownMenuItem>
//           <DropdownMenuItem>Settings</DropdownMenuItem>
//         </DropdownMenuGroup>
//         <DropdownMenuSeparator />
//         <DropdownMenuItem>Log out</DropdownMenuItem>
//       </DropdownMenuContent>
//     </DropdownMenu>
//   )
// }
