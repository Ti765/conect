
import type {Metadata} from 'next';
import {Geist, Geist_Mono} from 'next/font/google';
import './globals.css';
import { ThemeProvider } from '@/components/theme-provider';
import { AppHeader } from '@/components/layout/app-header';
import { AppSidebar } from '@/components/layout/app-sidebar';
import { SidebarProvider, SidebarInset } from '@/components/ui/sidebar';
import { Toaster } from "@/components/ui/toaster";

const geistSans = Geist({
  variable: '--font-geist-sans',
  subsets: ['latin'],
});

const geistMono = Geist_Mono({
  variable: '--font-geist-mono',
  subsets: ['latin'],
});

export const metadata: Metadata = {
  title: 'FiscalFlow',
  description: 'Gerenciamento fiscal inteligente com IA',
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="pt-BR" suppressHydrationWarning>
      <body className={`${geistSans.variable} ${geistMono.variable} antialiased font-sans`}>
        <ThemeProvider
          defaultTheme="dark"
          storageKey="fiscalflow-theme"
        >
          <SidebarProvider>
            <div className="flex min-h-screen bg-background text-foreground">
              <AppSidebar />
              <div className="flex flex-col flex-1 overflow-x-hidden"> {/* Added overflow-x-hidden */}
                <AppHeader />
                <SidebarInset> 
                  <main className="flex-1 p-4 md:p-6 lg:p-8">
                    {children}
                  </main>
                </SidebarInset>
              </div>
            </div>
            <Toaster />
          </SidebarProvider>
        </ThemeProvider>
      </body>
    </html>
  );
}
