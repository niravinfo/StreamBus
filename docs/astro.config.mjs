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
          label: 'Getting Started',
          items: [{ slug: 'guides/getting-started' }],
        },
        {
          label: 'Core',
          items: [
            { slug: 'guides/configuration' },
            { slug: 'guides/publishing' },
            { slug: 'guides/consuming' },
            { slug: 'guides/errors' },
          ],
        },
        {
          label: 'Advanced',
          items: [
            { slug: 'guides/topology' },
            { slug: 'guides/multi-bus' },
            { slug: 'guides/hosting' },
            { slug: 'guides/observability' },
            { slug: 'guides/tls' },
          ],
        },
        { slug: 'guides/changelog' },
      ],
    }),
  ],
});
