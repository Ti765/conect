import type {NextConfig} from 'next';

const securityHeaders = [
  {
    key: 'Content-Security-Policy',
    value: "object-src 'none'; script-src 'self' 'unsafe-eval' 'unsafe-inline';", // Ajuste 'unsafe-eval' e 'unsafe-inline' conforme necessário para produção
  },
  {
    key: 'Strict-Transport-Security',
    value: 'max-age=63072000; includeSubDomains; preload',
  },
  {
    key: 'X-Frame-Options',
    value: 'SAMEORIGIN',
  },
  {
    key: 'X-Content-Type-Options',
    value: 'nosniff',
  },
  {
    key: 'Referrer-Policy',
    value: 'origin-when-cross-origin',
  },
  {
    key: 'Permissions-Policy',
    value: 'camera=(), microphone=(), geolocation=(), payment=()' // Personalize conforme necessário
  },
  {
    key: 'Cross-Origin-Opener-Policy',
    value: 'same-origin-allow-popups' // ou 'same-origin' para maior restrição
  }
];

const nextConfig: NextConfig = {
  /* config options here */
  typescript: {
    ignoreBuildErrors: true,
  },
  eslint: {
    ignoreDuringBuilds: true,
  },
  images: {
    remotePatterns: [
      {
        protocol: 'https',
        hostname: 'placehold.co',
        port: '',
        pathname: '/**',
      },
    ],
  },
  async headers() {
    return [
      {
        source: '/:path*',
        headers: [
          {
            key: 'Access-Control-Allow-Origin',
            value: '*', // Considere restringir isso em produção
          },
          ...securityHeaders,
        ],
      },
    ];
  },
  allowedDevOrigins: [
    '3001-firebase-studio-1747941410841.cluster-ve345ymguzcd6qqzuko2qbxtfe.cloudworkstations.dev',
    '9003-firebase-studio-1747941410841.cluster-ve345ymguzcd6qqzuko2qbxtfe.cloudworkstations.dev'
  ],
};

export default nextConfig;