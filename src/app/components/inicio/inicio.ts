import { Component, inject, OnInit, signal } from '@angular/core';
import { MatButtonModule } from '@angular/material/button';
import { MatDividerModule } from "@angular/material/divider";
import { NavigationExtras, Router, RouterLink, RouterLinkActive, RouterOutlet } from '@angular/router';
import { MaterialToolbarModule } from '../../material/material.module';
import { MenuModel } from '../../models/menu-model';
import { LocalStorageService } from '../../services/local-storage-service';
import { ThemeService } from '../../services/theme-service';
import { DataApp } from '../../util/data-app';


@Component({
  selector: 'app-inicio',
  templateUrl: './inicio.html',
  styleUrl: './inicio.css',
  imports: [MaterialToolbarModule, RouterOutlet, MatDividerModule, RouterLink, RouterLinkActive, MatButtonModule],
})
export class Inicio implements OnInit {

  logged = signal<boolean>(false);
  isChecked = signal<boolean>(false);
  public isLightTheme = signal<boolean>(true);

  private localStorage = inject(LocalStorageService);
  private themeService = inject(ThemeService);

  private _router = inject(Router);

  public menusList = signal<Array<MenuModel>>([]);

  constructor() { }

  ngOnInit(): void {
    this.cargarMenusDeMantenimientos();
    this.validarUsuarioLogeado();

    var themex = this.themeService.getTheme();
    this.isLightTheme.update(val => themex == 'light');
  }

  onThemeSwitchChange() {
    this.themeService.toggleTheme();
  }

  routerActivated(value: any): void {
    this.validarUsuarioLogeado();
  }

  cargarMenusDeMantenimientos() {
    var nuevosMenus: Array<MenuModel> = [];
    nuevosMenus.push({ id: 1, ruta: '/menu/login', nombre: 'Iniciar Sesion' });
    nuevosMenus.push({ id: 2, ruta: '/menu/productos', nombre: 'Productos' });

    this.menusList.update(valores => [...nuevosMenus]);
  }


  homeClick() {
    let navigationExtras: NavigationExtras = {
      queryParams: {
        "nada": 'xd'
      }
    };
    this._router.navigate(['/menu/productos'], navigationExtras);
  }

  cerrarSesion() {
    let navigationExtras: NavigationExtras = {
      queryParams: {
        "logged": 'false'
      }
    };
    this.localStorage.setItem(DataApp.LOGGED, 'false');
    this._router.navigate(['/menu/login'], navigationExtras);
  }

  validarUsuarioLogeado() {
    const logged1 = this.localStorage.getItem(DataApp.LOGGED);
    this.logged.update(val => (logged1 == true));
  }

}
