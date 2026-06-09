import { ApplicationConfig, provideBrowserGlobalErrorListeners } from '@angular/core';
import { provideRouter, withComponentInputBinding } from '@angular/router';

import { MAT_DIALOG_DEFAULT_OPTIONS } from '@angular/material/dialog';
import { provideClientHydration, withEventReplay } from '@angular/platform-browser';
import { routes } from './app.routes';
import { DataService } from './services/data.service';
import { FormService } from './services/form-service';
import { LocalStorageService } from './services/local-storage-service';
import { ThemeService } from './services/theme-service';
import { UsuarioService } from './services/usuario-service';
import { CarritoService } from './services/carrito-service';

const services = [
  DataService,
  FormService,
  LocalStorageService,
  UsuarioService,
  ThemeService,
  CarritoService
]

export const appConfig: ApplicationConfig = {
  // providers: [
  //   provideBrowserGlobalErrorListeners(),
  //   provideRouter(routes), provideClientHydration(withEventReplay())
  // ]
  providers: [
    services,
    { provide: MAT_DIALOG_DEFAULT_OPTIONS, useValue: { hasBackdrop: false } },
    provideBrowserGlobalErrorListeners(),
    provideRouter(routes, withComponentInputBinding()),
    provideClientHydration(withEventReplay())
  ]
};
