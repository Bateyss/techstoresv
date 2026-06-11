import { Component, DOCUMENT, Inject, inject, OnInit, signal } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialog, MatDialogRef } from '@angular/material/dialog';
import { MatSnackBar } from '@angular/material/snack-bar';

import { Router } from '@angular/router';
import { Producto } from '../../models/producto';
import { DataService } from '../../services/data.service';
import { InventarioService } from '../../services/inventario-service';
import { Utils } from '../../util/utils';
import { MaterialTableDialogModule, MaterialTableModelModule } from '../../material/material.module';
import { MatDividerModule } from '@angular/material/divider';
import { DataApp } from '../../util/data-app';
import { MovimientoInventario } from '../../models/movimiento-inventario';

@Component({
  selector: 'app-inventarios',
  imports: [MaterialTableModelModule],
  templateUrl: './inventarios.html',
  styleUrl: './inventarios.css',
})
export class Inventarios implements OnInit {

  private _snackBar = inject(MatSnackBar);
  public innerWidths = '0';
  private document = inject(DOCUMENT);
  private dialog = inject(MatDialog);

  private inventarioService = inject(InventarioService);
  private dataService = inject(DataService);

  public productosList = signal<Array<Producto>>([]);

  constructor(private _router: Router,) { }

  ngOnInit(): void {
    this.innerWidths = (this.document.body.clientWidth * 0.9) + 'px';
    this.cargarListas();
  }

  cargarListas() {
    this.productosList.update(valores => this.dataService.getProductos());
  }

  verMovimientosProducto(producto: Producto) {

    const dialogConfig = Utils.getMatDialogConf()
    dialogConfig.data = {
      producto: producto
    };

    const dialogRef = this.dialog.open(MovimientosProductoDialog, dialogConfig);
    dialogRef.afterClosed().subscribe(
      result => {
        this.cargarListas();
      });
  }

}

@Component({
  selector: 'dialog-detalles',
  imports: [MaterialTableDialogModule],
  templateUrl: './movimientos.producto.html',
  styleUrl: './inventarios.css'
})
export class MovimientosProductoDialog implements OnInit {

  public innerWidths = '0';
  private document = inject(DOCUMENT);

  readonly dialogRef = inject(MatDialogRef<MovimientosProductoDialog>);

  private inventarioService = inject(InventarioService);

  private producto = signal<Producto>(DataApp.productoVacio());

  public movimientosList = signal<Array<MovimientoInventario>>([]);
  public pedido = DataApp.pedidoVacio();

  constructor(@Inject(MAT_DIALOG_DATA) public data: {
    producto: Producto
  }) {
    this.innerWidths = (this.document.body.clientWidth * 0.9) + 'px';
    this.producto.update(valor => data.producto);
  }

  ngOnInit() {
    this.cargarListas();
  }

  cargarListas() {
    this.movimientosList.update(valores => [...this.inventarioService.getMovimientosProducto(this.producto())]);
  }

  cerrarDetalles() {
    this.dialogRef.close();
  }

}
