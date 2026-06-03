import { Component, DOCUMENT, inject, Inject } from '@angular/core';
import { MatButtonModule } from '@angular/material/button';
import { MatDialog, MatDialogConfig } from '@angular/material/dialog';
import { MatIconModule } from '@angular/material/icon';
import { MatMenuModule } from '@angular/material/menu';
import { MatToolbarModule } from '@angular/material/toolbar';
import {MatSlideToggleModule} from '@angular/material/slide-toggle';
import { NavigationExtras, Router, RouterOutlet } from '@angular/router';
import { Menu } from '../menu/menu';
import { MaterialModule } from '../../material/material.module';
import { LocalStorageService } from '../../services/local-storage-service';


@Component({
  selector: 'app-inicio',
  templateUrl: './inicio.html',
  styleUrl: './inicio.css',
  imports: [MaterialModule, RouterOutlet],
})
export class Inicio {

  logged = false;
  isChecked = false;
  public isLightTheme = true;

  private dialog = inject(MatDialog);
  private localStorage = inject(LocalStorageService);

  constructor(
    private _router: Router) {
    var logged1 = this.localStorage.getItem('logged');
    this.logged = logged1 == 'true' ? true : false;
  }

  onThemeSwitchChange() {
    this.isLightTheme = !this.isChecked;

    document.body.setAttribute(
      'data-theme',
      this.isLightTheme ? 'light' : 'dark'
    );
  }

  routerActivated(value: any): void {
    var logged1 = this.localStorage.getItem('logged');
    this.logged = logged1 == 'true' ? true : false;
  }

  onLoggionChangue(value: any): void {
    console.log('valor loggin changue');
    console.log(value);
    if (value) {
      this.logged = true;
    } else {
      this.logged = false;
    }
  }

  abrirMenu() {
    const dialogConfig = new MatDialogConfig();
    dialogConfig.disableClose = false;
    dialogConfig.autoFocus = true;
    dialogConfig.data = {
      idUsuario: 1
    };
    const dialogRef = this.dialog.open(Menu, dialogConfig);
    dialogRef.afterClosed().subscribe(
      result => {
        if (result && result?.url) {
          let navigationExtras: NavigationExtras = {
            queryParams: {
              "nada": 'xd'
            }
          };
          this._router.navigate([result.url], navigationExtras);
        }
      });
  }

  homeClick() {
    let navigationExtras: NavigationExtras = {
      queryParams: {
        "nada": 'xd'
      }
    };
    this._router.navigate(['/menu/planilla'], navigationExtras);
  }

  cerrarSesion() {
    let navigationExtras: NavigationExtras = {
      queryParams: {
        "logged": 'false'
      }
    };
    this.localStorage.setItem('logged', 'false');
    this._router.navigate(['/menu/login'], navigationExtras);
  }

}
