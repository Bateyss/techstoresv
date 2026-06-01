import { Component, Inject, inject } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { MaterialModule } from '../../material/material.module';
import { MenuModel } from '../../models/menu-model';

@Component({
  selector: 'app-menu',
  imports: [MaterialModule],
  templateUrl: './menu.html',
  styleUrl: './menu.css',
})
export class Menu {

  readonly dialogRef = inject(MatDialogRef<Menu>);

  public menusList: Array<MenuModel> = [];
  public menuSeleccionado: MenuModel = {
    id: 0,
    ruta: '',
    nombre: ''
  };
  public innerWidths = '0';

  constructor(
    @Inject(MAT_DIALOG_DATA) public data: {
      idUsuario: number
    }
  ) { this.innerWidths = (window.innerWidth * 0.9) + 'px'; }

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
