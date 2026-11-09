import { defineCollection } from 'astro:content';
import { docsLoader, i18nLoader } from '@astrojs/starlight/loaders';
import { docsSchema, i18nSchema } from '@astrojs/starlight/schema';

export const collections = {
	docs: defineCollection({ loader: docsLoader(), schema: docsSchema() }),
	// Optional: the i18n collection is used to translate UI in multilingual sites
	i18n: defineCollection({ loader: i18nLoader(), schema: i18nSchema() }),
};
