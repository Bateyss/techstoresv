import { DOCUMENT, effect, inject, Injectable, signal } from '@angular/core';
import { LocalStorageService } from './local-storage-service';

export type AppTheme = 'light' | 'dark';

@Injectable({
  providedIn: 'root',
})
export class ThemeService {

  private documento = inject(DOCUMENT);
  private localStorageService = inject(LocalStorageService);

  theme = signal<AppTheme>('light');

  constructor() {
    effect(() => {
      const currentThene = this.theme;
      this.localStorageService.setItem('app-theme', currentThene()?currentThene():'light');

      const rootElement = this.documento.documentElement;

      if (currentThene() === 'dark') {
        rootElement.classList.add('dark');
        rootElement.classList.remove('light');
      } else {
        rootElement.classList.add('light');
        rootElement.classList.remove('dark');
      }
    });
  }

  toggleTheme() {
    this.theme.update(current => current === 'light' ? 'dark' : 'light');
  }

  setTheme(newTheme: AppTheme) {
    this.theme.set(newTheme);
  }

}
