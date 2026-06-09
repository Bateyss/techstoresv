import { isPlatformBrowser } from '@angular/common';
import { inject, Injectable, InjectionToken, PLATFORM_ID } from '@angular/core';

export const LOCAL_STORAGE = new InjectionToken<Storage>('WindowLocalStorage', {
  providedIn: 'root',
  factory: () => {
    const platformId = inject(PLATFORM_ID);

    if (isPlatformBrowser(platformId)) {
      return localStorage;
    }

    return {
      length: 0,
      clear: () => { },
      getItem: () => null,
      key: () => null,
      removeItem: () => { },
      setItem: () => { },
    };
  },
})

@Injectable({
  providedIn: 'root',
})
export class LocalStorageService {

  private storage = inject(LOCAL_STORAGE);

  setItem(key: string, value: unknown): void {
    const txtValue = typeof value === 'string' ? value : JSON.stringify(value);
    this.storage.setItem(key, txtValue);
  }

  getItem<T>(key: string): T | null {
    const value = this.storage.getItem(key);
    if (!value) return null;

    try { return JSON.parse(value) as T }
    catch { return value as unknown as T }
  }

  removeItem(key: string): void {
    this.storage.removeItem(key);
  }

  clear(): void {
    this.storage.clear();
  }

}
