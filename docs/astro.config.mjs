import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import { starlightBasePath } from 'starlight-base-path';

// https://astro.build/config
export default defineConfig({
  // GitHub Pages project site: must match the repo name.
  site: 'https://niravinfo.github.io',
  base: '/StreamBus',
  integrations: [
    starlight({
      plugins: [starlightBasePath()],
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
          label: 'Concepts',
          items: [
            { slug: 'concepts/introduction' },
            { slug: 'concepts/streams' },
            { slug: 'concepts/messages' },
            { slug: 'concepts/producers' },
            { slug: 'concepts/consumers' },
            { slug: 'concepts/middleware' },
            { slug: 'concepts/exceptions' },
          ],
        },
        {
          label: 'Guides',
          items: [
            { slug: 'guides/get-started' },
            { slug: 'guides/publish-an-event' },
            { slug: 'guides/consume-events' },
            { slug: 'guides/handle-failures' },
            { slug: 'guides/scale-out' },
            { slug: 'guides/partition-a-stream' },
            { slug: 'guides/use-multiple-buses' },
            { slug: 'guides/replay-a-stream' },
            { slug: 'guides/add-cross-cutting-logic' },
          ],
        },
        {
          label: 'Configuration',
          items: [
            { slug: 'configuration/host-and-connections' },
            { slug: 'configuration/endpoint-options' },
            { slug: 'configuration/retry-and-dead-letters' },
            { slug: 'configuration/observability' },
            { slug: 'configuration/transport-security' },
          ],
        },
      ],
    }),
  ],
});
