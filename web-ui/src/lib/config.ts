function trimTrailingSlash(value: string): string {
  return value.replace(/\/+$/, '');
}

function resolveApiBase(): string {
  const configured = (import.meta.env.VITE_API_BASE as string | undefined)?.trim();
  if (configured && configured.length > 0) {
    return trimTrailingSlash(configured);
  }

  // Default to same-origin + Vite proxy (/api -> backend).
  return '/api';
}

function resolveWebSocketUrl(apiBase: string): string {
  const configured = (import.meta.env.VITE_WS_URL as string | undefined)?.trim();
  if (configured && configured.length > 0) {
    if (configured.startsWith('ws://') || configured.startsWith('wss://')) {
      return configured;
    }
    if (configured.startsWith('http://') || configured.startsWith('https://')) {
      const asWs = configured.replace(/^http/, 'ws');
      return asWs.endsWith('/ws') ? asWs : `${trimTrailingSlash(asWs)}/ws`;
    }
    if (configured.startsWith('/')) {
      const protocol = globalThis.location.protocol === 'https:' ? 'wss:' : 'ws:';
      return `${protocol}//${globalThis.location.host}${configured}`;
    }
    const protocol = globalThis.location.protocol === 'https:' ? 'wss:' : 'ws:';
    return `${protocol}//${trimTrailingSlash(configured)}/ws`;
  }

  const url = new URL(apiBase, globalThis.location.origin);
  const wsProtocol = url.protocol === 'https:' ? 'wss:' : 'ws:';

  // If API is relative (/api), keep WS on same origin and pass through /api proxy.
  if (!apiBase.startsWith('http://') && !apiBase.startsWith('https://')) {
    const proxyPath = `${trimTrailingSlash(url.pathname)}/ws`;
    return `${wsProtocol}//${url.host}${proxyPath.startsWith('/') ? proxyPath : `/${proxyPath}`}`;
  }

  // For absolute API base, connect directly to backend WS endpoint.
  const wsPathBase = url.pathname.replace(/\/api\/?$/, '');
  const wsPath = `${trimTrailingSlash(wsPathBase)}/ws`;
  return `${wsProtocol}//${url.host}${wsPath.startsWith('/') ? wsPath : `/${wsPath}`}`;
}

export const API_BASE = resolveApiBase();
export const WS_URL = resolveWebSocketUrl(API_BASE);

export const WS_ENABLED =
  ((import.meta.env.VITE_WS_ENABLED as string | undefined)?.trim().toLowerCase() ?? 'true') !== 'false';
