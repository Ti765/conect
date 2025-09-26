"use client";

import React, { useId } from "react";

/**
 * FiscalFlow — FF Flow Italic (ícone em SVG)
 *
 * Props:
 * - size: número (largura e altura em px). Default 96
 * - title: string para acessibilidade
 * - className: classes (ex. Tailwind)
 * - idSuffix: string opcional p/ evitar colisão de IDs do gradient quando
 *   renderizar várias instâncias na mesma página
 */
export default function FiscalFlowFFItalic({
  size = 96,
  title = "FiscalFlow — FF Flow Italic",
  className,
  idSuffix,
  ...props
}) {
  const autoId = useId();
  const gradId = `ff-gradient-${idSuffix ?? autoId}`;

  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 96 96"
      role="img"
      aria-label={title}
      className={className}
      {...props}
    >
      <title>{title}</title>
      <defs>
        <linearGradient id={gradId} x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" stopColor="#ff6a6a" />
          <stop offset="50%" stopColor="#7b46ff" />
          <stop offset="100%" stopColor="#44c3ff" />
        </linearGradient>
      </defs>

      {/* FF Italic */}
      <g transform="skewX(-10)">
        {/* F frontal (cheio) */}
        <path
          d="M20 20 h34 a6 6 0 0 1 0 12 H32 v10 h18 a6 6 0 0 1 0 12 H20 Z"
          fill={`url(#${gradId})`}
          opacity={0.95}
        />
        {/* F posterior (semi) */}
        <path
          d="M46 20 h30 a6 6 0 0 1 0 12 H58 v10 h12 a6 6 0 0 1 0 12 H46 Z"
          fill={`url(#${gradId})`}
          opacity={0.35}
        />
      </g>

      {/* Linha de fluxo */}
      <path
        d="M14 64 C 36 50, 56 78, 80 42"
        fill="none"
        stroke={`url(#${gradId})`}
        strokeWidth={6}
        strokeLinecap="round"
      />
    </svg>
  );
}
