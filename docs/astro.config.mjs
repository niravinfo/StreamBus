import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// https://astro.build/config
export default defineConfig({
  // GitHub Pages project site: must match the repo name.
  site: 'https://niravinfo.github.io',
  base: '/StreamBus',
  integrations: [
    starlight({
      title: 'StreamBus',
      description: 'A modern .NET client for RabbitMQ Streams.',
      social: [
        {
          icon: 'github',
          label: 'GitHub',
          href: 'https://github.com/niravinfo/StreamBus',
        },
      ],
      sidebar: [
        {
          label: 'Get started',
          items: [
            { slug: 'introduction' },
            { slug: 'installation' },
            { slug: 'quickstart' },
          ],
        },
        {
          label: 'Fundamentals',
          items: [
            { slug: 'architecture' },
            { slug: 'configuration' },
            { slug: 'topology' },
            { slug: 'lifecycle' },
          ],
        },
        {
          label: 'Messaging',
          items: [
            { slug: 'publishers' },
            { slug: 'consumers' },
            { slug: 'error-handling' },
          ],
        },
        {
          label: 'In production',
          items: [
            { slug: 'transport-security' },
            { slug: 'observability' },
            { slug: 'multiple-buses' },
            { slug: 'middleware' },
          ],
        },
        {
          label: 'API reference',
          items: [{ autogenerate: { directory: 'api' } }],
        },
      ],
    }),
  ],
});
