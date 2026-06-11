import { inject, Injectable } from '@angular/core';
import { MatSnackBar } from '@angular/material/snack-bar';
import { DataService } from './data.service';
import { Producto } from '../models/producto';
import { MovimientoInventario } from '../models/movimiento-inventario';

@Injectable({
  providedIn: 'root',
})
export class InventarioService {

  private dataService = inject(DataService);
  private _snackBar = inject(MatSnackBar);

  constructor() { }

  getMovimientosProducto(producto: Producto): Array<MovimientoInventario> {
    let movimientos = this.dataService.getMovimientosInventario();
    let movimmientosProducto: Array<MovimientoInventario> = [];

    for (const movimiento of movimientos) {
      if (movimiento.producto.id == producto.id) {
        movimmientosProducto.push(movimiento);
      }
    }

    return movimmientosProducto;
  }


}
