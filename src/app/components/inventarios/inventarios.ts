import { Component, DOCUMENT, Inject, inject, OnInit, signal } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialog, MatDialogRef } from '@angular/material/dialog';

import { Router } from '@angular/router';
import { MaterialTableDialogModule, MaterialTableModelModule } from '../../material/material.module';
import { MovimientoInventario } from '../../models/movimiento-inventario';
import { Producto } from '../../models/producto';
import { TipoMovimiento } from '../../models/tipo-movimiento';
import { DataService } from '../../services/data.service';
import { InventarioService } from '../../services/inventario-service';
import { DataApp } from '../../util/data-app';
import { Utils } from '../../util/utils';

@Component({
  selector: 'app-inventarios',
  imports: [MaterialTableModelModule],
  templateUrl: './inventarios.html',
  styleUrl: './inventarios.css',
})
export class Inventarios implements OnInit {

  public innerWidths = '0';
  private document = inject(DOCUMENT);
  private dialog = inject(MatDialog);

  private dataService = inject(DataService);

  public productosList = signal<Array<Producto>>([]);
  public tipoMovimientoList = signal<Array<TipoMovimiento>>([]);

  constructor(private _router: Router,) { }

  ngOnInit(): void {
    this.innerWidths = (this.document.body.clientWidth * 0.9) + 'px';
    this.cargarListas();
  }

  cargarListas() {
    this.productosList.update(valores => this.dataService.getProductos());
    this.tipoMovimientoList.update(valores => this.dataService.getTiposMovimiento());
  }

  verCompras() {
    this.verMovimientosTipo(this.tipoMovimientoList()[this.tipoMovimientoList().findIndex(a => a.id == 1)]);
  }

  verVentas() {
    this.verMovimientosTipo(this.tipoMovimientoList()[this.tipoMovimientoList().findIndex(a => a.id == 2)]);
  }

  verMovimientosProducto(producto: Producto) {
    const dialogConfig = Utils.getMatDialogConf()
    dialogConfig.data = {
      producto: producto
    };
    this.dialog.open(MovimientosProductoDialog, dialogConfig);
  }

  verMovimientosTipo(tipoMovimiento: TipoMovimiento) {
    const dialogConfig = Utils.getMatDialogConf()
    dialogConfig.data = {
      tipoMovimiento: tipoMovimiento
    };
    this.dialog.open(MovimientosTipoDialog, dialogConfig);
  }

  verMovimientosDiario() {
    const dialogConfig = Utils.getMatDialogConf()
    this.dialog.open(MovimientosLibroDiarioDialog, dialogConfig);
  }

  verMovimientosMayor() {
    const dialogConfig = Utils.getMatDialogConf()
    this.dialog.open(MovimientosLibroMayorDialog, dialogConfig);
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

@Component({
  selector: 'dialog-detalles-tipo',
  imports: [MaterialTableDialogModule],
  templateUrl: './movimientos.tipo.html',
  styleUrl: './inventarios.css'
})
export class MovimientosTipoDialog implements OnInit {

  public innerWidths = '0';
  private document = inject(DOCUMENT);

  readonly dialogRef = inject(MatDialogRef<MovimientosTipoDialog>);

  private inventarioService = inject(InventarioService);

  public tipoMovimiento = signal<TipoMovimiento>(DataApp.tipoMovimientoVacio());

  public movimientosList = signal<Array<MovimientoInventario>>([]);

  constructor(@Inject(MAT_DIALOG_DATA) public data: {
    tipoMovimiento: TipoMovimiento
  }) {
    this.innerWidths = (this.document.body.clientWidth * 0.9) + 'px';
    this.tipoMovimiento.update(valor => data.tipoMovimiento);
  }

  ngOnInit() {
    this.cargarListas();
  }

  cargarListas() {
    this.movimientosList.update(valores => [...this.inventarioService.getMovimientosTipoMovimiento(this.tipoMovimiento())]);
  }

  cerrarDetalles() {
    this.dialogRef.close();
  }

}

@Component({
  selector: 'dialog-detalles-tipo',
  imports: [MaterialTableDialogModule],
  templateUrl: './movimientos.diario.html',
  styleUrl: './inventarios.css'
})
export class MovimientosLibroDiarioDialog implements OnInit {

  public innerWidths = '0';
  private document = inject(DOCUMENT);

  readonly dialogRef = inject(MatDialogRef<MovimientosLibroDiarioDialog>);

  private dataService = inject(DataService);

  public movimientosList = signal<Array<MovimientoInventario>>([]);

  constructor() {
    this.innerWidths = (this.document.body.clientWidth * 0.9) + 'px';
  }

  ngOnInit() {
    this.cargarListas();
  }

  cargarListas() {
    this.movimientosList.update(valores => [...this.dataService.getMovimientosInventario()]);
  }

  cerrarDetalles() {
    this.dialogRef.close();
  }

}

@Component({
  selector: 'dialog-detalles-tipo',
  imports: [MaterialTableDialogModule],
  templateUrl: './movimientos.mayor.html',
  styleUrl: './inventarios.css'
})
export class MovimientosLibroMayorDialog implements OnInit {

  public innerWidths = '0';
  private document = inject(DOCUMENT);

  readonly dialogRef = inject(MatDialogRef<MovimientosLibroMayorDialog>);

  private dataService = inject(DataService);

  public movimientosList = signal<Array<MovimientoInventario>>([]);
  public tipoMovimientoList = signal<Array<TipoMovimiento>>([]);

  constructor() {
    this.innerWidths = (this.document.body.clientWidth * 0.9) + 'px';
  }

  ngOnInit() {
    this.cargarListas();
  }

  cargarListas() {
    this.movimientosList.update(valores => [...this.dataService.getMovimientosInventario()]);
    this.tipoMovimientoList.update(valores => [...this.dataService.getTiposMovimiento()]);
  }

  movimientosPorTipo(tipo: TipoMovimiento): Array<MovimientoInventario> {
    let movimientos = this.movimientosList();
    let movimientosPorTipo: Array<MovimientoInventario> = [];

    for (const movimiento of movimientos) {
      if (movimiento.tipo_movimiento.id == tipo.id) {
        movimientosPorTipo.push(movimiento);
      }
    }
    return movimientosPorTipo;
  }

  cerrarDetalles() {
    this.dialogRef.close();
  }

}
