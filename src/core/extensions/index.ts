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

const existing = globalScope[GLOBAL_EXTENSION_MANAGER_KEY];
let extensionManager: ExtensionManager;
if (isExtensionManagerLike(existing)) {
  extensionManager = existing;
} else {
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
  globalScope[GLOBAL_EXTENSION_MANAGER_KEY] = next;
  extensionManager = next;
}

export default extensionManager;
