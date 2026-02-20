import { ExtensionManager } from './manager';

export * from './manager';
export * from './extension';

/**
 * Global extension manager singleton instance.
 */
const GLOBAL_EXTENSION_MANAGER_KEY = '__twe_extension_manager_v1';
const EXTENSION_MANAGER_SIGNATURE = 'twitter-web-exporter-extension-manager-v1';
const EXTENSION_MANAGER_REVISION = 3;
const globalScope = globalThis as Record<string, unknown>;

function collectGlobalScopes(): Array<Record<string, unknown>> {
  const out: Array<Record<string, unknown>> = [];
  const seen = new Set<unknown>();
  const push = (candidate: unknown) => {
    if (!candidate || typeof candidate !== 'object') return;
    if (seen.has(candidate)) return;
    seen.add(candidate);
    out.push(candidate as Record<string, unknown>);
  };

  push(globalThis);
  if (typeof window !== 'undefined') {
    push(window);
    const asWindow = window as unknown as {
      unsafeWindow?: unknown;
      wrappedJSObject?: unknown;
    };
    push(asWindow.unsafeWindow);
    push(asWindow.wrappedJSObject);
  }

  const globalUnsafeWindow = (globalThis as Record<string, unknown>).unsafeWindow;
  push(globalUnsafeWindow);
  if (globalUnsafeWindow && typeof globalUnsafeWindow === 'object') {
    push((globalUnsafeWindow as Record<string, unknown>).wrappedJSObject);
  }

  return out;
}

function getGlobalManagerCandidate(): unknown {
  for (const scope of collectGlobalScopes()) {
    const candidate = scope[GLOBAL_EXTENSION_MANAGER_KEY];
    if (candidate) {
      return candidate;
    }
  }
  return undefined;
}

function publishGlobalManager(manager: ExtensionManager): void {
  for (const scope of collectGlobalScopes()) {
    try {
      scope[GLOBAL_EXTENSION_MANAGER_KEY] = manager;
    } catch {
      // ignore
    }
  }
}

function isExtensionManagerLike(candidate: unknown): candidate is ExtensionManager {
  if (!candidate || typeof candidate !== 'object') return false;
  const manager = candidate as {
    __twe_extension_manager_signature_v1?: string;
    __twe_extension_manager_revision_v1?: number;
    add?: (ctor: unknown) => void;
    start?: () => void;
    dispose?: () => void;
    uninstallHooks?: () => void;
    signal?: unknown;
    isDisposed?: () => boolean;
  };
  return (
    manager.__twe_extension_manager_signature_v1 === EXTENSION_MANAGER_SIGNATURE &&
    Number(manager.__twe_extension_manager_revision_v1) === EXTENSION_MANAGER_REVISION &&
    typeof manager.add === 'function' &&
    typeof manager.start === 'function' &&
    typeof manager.dispose === 'function' &&
    typeof manager.uninstallHooks === 'function' &&
    typeof manager.signal === 'object' &&
    (typeof manager.isDisposed !== 'function' || !manager.isDisposed())
  );
}

let extensionManager: ExtensionManager | null = null;

export function getExtensionManager(): ExtensionManager {
  if (extensionManager && !extensionManager.isDisposed()) {
    return extensionManager;
  }

  const existing = getGlobalManagerCandidate();
  if (isExtensionManagerLike(existing)) {
    extensionManager = existing;
    publishGlobalManager(extensionManager);
    return extensionManager;
  }

  if (existing && typeof existing === 'object') {
    try {
      const oldManager = existing as {
        dispose?: () => void;
        uninstallHooks?: () => void;
        uninstall?: () => void;
      };
      if (typeof oldManager.dispose === 'function') {
        oldManager.dispose();
      }
      if (typeof oldManager.uninstallHooks === 'function') {
        oldManager.uninstallHooks();
      }
      if (typeof oldManager.uninstall === 'function') {
        oldManager.uninstall();
      }
    } catch {
      // ignore
    }
  }

  const next = new ExtensionManager();
  publishGlobalManager(next);
  globalScope[GLOBAL_EXTENSION_MANAGER_KEY] = next;
  extensionManager = next;
  return extensionManager;
}

const extensionManagerProxy = new Proxy({} as ExtensionManager, {
  get(_target, prop, receiver) {
    const manager = getExtensionManager();
    const value = Reflect.get(manager, prop, receiver);
    return typeof value === 'function' ? value.bind(manager) : value;
  },
  set(_target, prop, value, receiver) {
    const manager = getExtensionManager();
    return Reflect.set(manager, prop, value, receiver);
  },
});

export default extensionManagerProxy;
