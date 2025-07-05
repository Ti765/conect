
import type { SVGProps } from 'react';

export function FiscalFlowLogo(props: SVGProps<SVGSVGElement>) {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      viewBox="0 0 180 40"
      width="180"
      height="40"
      aria-label="FiscalFlow Logo"
      {...props}
    >
      <defs>
        <linearGradient id="fiscalFlowGradient" x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" style={{ stopColor: 'hsl(var(--destructive))', stopOpacity: 1 }} />
          <stop offset="50%" style={{ stopColor: 'hsl(var(--primary))', stopOpacity: 1 }} />
          <stop offset="100%" style={{ stopColor: 'hsl(var(--accent))', stopOpacity: 1 }} />
        </linearGradient>
      </defs>
      <text
        x="50%"
        y="50%"
        dominantBaseline="middle"
        textAnchor="middle"
        fontSize="28"
        fontFamily="var(--font-geist-sans), Arial, sans-serif"
        fontWeight="bold"
        fill="url(#fiscalFlowGradient)"
      >
        FiscalFlow
      </text>
    </svg>
  );
}
