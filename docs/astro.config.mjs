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
      description: 'MassTransit-style client for RabbitMQ streams on .NET',
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
          ],
        },
        {
          label: 'Advanced',
          items: [
            { slug: 'guides/errors' },
            { slug: 'guides/observability' },
          ],
        },
        { slug: 'guides/changelog' },
      ],
    }),
  ],
});
