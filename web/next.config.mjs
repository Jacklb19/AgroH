/** @type {import('next').NextConfig} */
const nextConfig = {
  async redirects() {
    return [
      { source: "/dashboards", destination: "/datos", permanent: true },
      { source: "/impacto",    destination: "/",      permanent: true },
    ];
  },
};
export default nextConfig;
