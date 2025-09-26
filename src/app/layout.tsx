import type { Metadata } from 'next';
import { Geist, Geist_Mono } from 'next/font/google';
import './globals.css';
import { ThemeProvider } from '@/components/theme-provider';
import { AppHeader } from '@/components/layout/app-header';
import { AppSidebar } from '@/components/layout/app-sidebar';
import { SidebarProvider, SidebarInset } from '@/components/ui/sidebar';
import { Toaster } from '@/components/ui/toaster';

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
  icons: {
    icon: [
      { url: '/favicon.ico', sizes: 'any' },
      { url: '/favicon-16.png', type: 'image/png', sizes: '16x16' },
      { url: '/favicon-32.png', type: 'image/png', sizes: '32x32' },
      { url: '/favicon-48.png', type: 'image/png', sizes: '48x48' },
      { url: '/favicon-64.png', type: 'image/png', sizes: '64x64' },
      { url: '/favicon-128.png', type: 'image/png', sizes: '128x128' },
      { url: '/favicon-256.png', type: 'image/png', sizes: '256x256' },
    ],
    shortcut: ['/favicon.ico'],
  },
  // Se quiser, adicione themeColor por esquema de cor:
  // themeColor: [
  //   { media: '(prefers-color-scheme: light)', color: '#ffffff' },
  //   { media: '(prefers-color-scheme: dark)', color: '#0b0f1a' },
  // ],
};

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="pt-BR" suppressHydrationWarning>
      <body className={`${geistSans.variable} ${geistMono.variable} antialiased font-sans`}>
        <ThemeProvider defaultTheme="dark" storageKey="fiscalflow-theme">
          <SidebarProvider>
            <div className="flex min-h-screen bg-background text-foreground">
              <AppSidebar />
              <div className="flex flex-col flex-1 overflow-x-hidden">
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
