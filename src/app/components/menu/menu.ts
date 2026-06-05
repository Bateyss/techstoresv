import { CommonModule } from '@angular/common';
import { Component, DOCUMENT, Inject, inject, OnInit } from '@angular/core';
import { MatButtonModule } from '@angular/material/button';
import { MatCardModule } from '@angular/material/card';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { MatIconModule } from '@angular/material/icon';
import { MenuModel } from '../../models/menu-model';

@Component({
  selector: 'app-menu',
  imports: [MatCardModule, MatButtonModule, MatIconModule, CommonModule],
  templateUrl: './menu.html',
  styleUrl: './menu.css',
})
export class Menu implements OnInit {

  readonly dialogRef = inject(MatDialogRef<Menu>);

  public menusList: Array<MenuModel> = [];
  public menuSeleccionado: MenuModel = {
    id: 0,
    ruta: '',
    nombre: ''
  };
  public innerWidths = '0';

  private document =  inject(DOCUMENT);

  constructor(
    @Inject(MAT_DIALOG_DATA) public data: {
      idUsuario: number
    }
  ) { this.innerWidths = (this.document.body.clientWidth * 0.9) + 'px'; }

  ngOnInit(): void {
    this.cargarMenusDeMantenimientos();
  }

  cargarMenusDeMantenimientos() {
    this.menusList.push({ id: 1, ruta: '/menu/login', nombre: 'LogIn' });
    this.menusList.push({ id: 2, ruta: '/menu/productos', nombre: 'Productos' });
  }

  redirigirMenu(dir: string) {
    let response: any = {};
    response.url = dir;
    this.dialogRef.close(response);
  }
}
