import en from '@/i18n/en.json';

type Dictionary = Record<string, string>;

const dictionaries: Record<string, Dictionary> = {
  en,
};

function resolveLocale() {
  const webapp = (window as Window & {
    Telegram?: { WebApp?: { initDataUnsafe?: { user?: { language_code?: string } } } };
  }).Telegram?.WebApp;
  const code = webapp?.initDataUnsafe?.user?.language_code || 'en';
  const normalized = code.toLowerCase().split('-')[0];
  return dictionaries[normalized] ? normalized : 'en';
}

export function t(key: string, fallback?: string) {
  const locale = resolveLocale();
  return dictionaries[locale]?.[key] || fallback || key;
}
