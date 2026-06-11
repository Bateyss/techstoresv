import { inject, Injectable } from '@angular/core';
import { DetalleCompra } from '../models/detalle-compra';
import { LoteInventario } from '../models/lote-inventario';
import { MovimientoInventario } from '../models/movimiento-inventario';
import { DataApp } from '../util/data-app';
import { DataService } from './data.service';
import { LocalStorageService } from './local-storage-service';

@Injectable({
  providedIn: 'root',
})
export class CompraService {

  private localStorageService = inject(LocalStorageService);
  private dataService = inject(DataService);

  constructor() { }

  getdetalleCompras(): Array<DetalleCompra> {
    var detalleCompraList: Array<DetalleCompra> = [];

    var detalleCompraListSession = this.localStorageService.getItem<Array<DetalleCompra>>(DataApp.DETALLE_PEDIDOS_COMPRA_ID);
    if (detalleCompraListSession) {
      detalleCompraList = detalleCompraListSession;
    }

    return detalleCompraList;
  }

  eliminarDetallesPedidoCompra() {
    this.localStorageService.removeItem(DataApp.DETALLE_PEDIDOS_COMPRA_ID);
  }



  registrarCompra() {
    var detalles = this.getdetalleCompras();
    for (const det of detalles) {
      this.registrarCompraInventario(det);
    }
    this.eliminarDetallesPedidoCompra();
  }

  registrarCompraInventario(detalleCompra: DetalleCompra) {

    // crear lote de compra
    let lote: LoteInventario = DataApp.loteInventarioVacio();
    lote.cantidad_actual = detalleCompra.cantidad;
    lote.cantidad_inicial = detalleCompra.cantidad;
    lote.costo_unitario = detalleCompra.precio_unitario_venta;
    lote.fecha_ingreso = new Date();
    lote.producto = detalleCompra.producto;

    lote = this.dataService.pushLoteInventario(lote);

    // guardar movimiento de inventario de compra
    let tipos = this.dataService.getTiposMovimiento();
    let movimiento: MovimientoInventario = DataApp.movimientoInventarioVacio();
    movimiento.cantidad = detalleCompra.cantidad;
    movimiento.fecha = new Date();
    movimiento.producto = detalleCompra.producto;
    movimiento.tipo_movimiento = tipos[tipos.findIndex(a => a.id == 1)];
    movimiento.lote = lote;

    this.dataService.pushMovimientoInventario(movimiento);

    // actualizar productos
    if (detalleCompra.producto.stock_web && detalleCompra.producto.stock_web > 0) {
      detalleCompra.producto.stock_web += detalleCompra.cantidad_web;
    } else {
      detalleCompra.producto.stock_web = detalleCompra.cantidad_web;
    }

    if (detalleCompra.producto.stock_local && detalleCompra.producto.stock_local > 0) {
      detalleCompra.producto.stock_local += detalleCompra.cantidad_local;
    } else {
      detalleCompra.producto.stock_local = detalleCompra.cantidad_local;
    }

    this.dataService.editarProducto(detalleCompra.producto);
  }

  aumentarDetalleCompra(detalle: DetalleCompra): Array<DetalleCompra> {
    var detalles = this.getdetalleCompras();
    for (const deta of detalles) {
      if (deta.id == detalle.id) {
        deta.cantidad++;
        deta.cantidad_web++;
        this.editarDetalleCompra(deta);
      }
    }
    return detalles;
  }

  disminuirDetalleCompra(detalle: DetalleCompra): Array<DetalleCompra> {
    var detalles = this.getdetalleCompras();
    for (const deta of detalles) {
      if (deta.id == detalle.id && deta.cantidad > 0) {
        deta.cantidad--;
        deta.cantidad_web--;
        this.editarDetalleCompra(deta);
      }
    }
    return detalles;
  }

  editarDetalleCompra(datos: DetalleCompra) {
    var detalleList: Array<DetalleCompra> = this.getdetalleCompras();
    var detalleListNueva: Array<DetalleCompra> = [];

    detalleList.forEach(arrData => {
      var newData: DetalleCompra = arrData;
      if (newData.id == datos.id) {
        newData = datos;
      }
      detalleListNueva.push(newData);
    });

    this.localStorageService.setItem(DataApp.DETALLE_PEDIDOS_COMPRA_ID, detalleListNueva);
  }

  pushDetalleCompra(datos: DetalleCompra) {
    var detalleCompraList: Array<DetalleCompra> = this.getdetalleCompras();
    if (detalleCompraList.length > 0) {
      datos.id = detalleCompraList[detalleCompraList.length - 1].id + 1;
    } else {
      datos.id = 1;
    }
    detalleCompraList.push(datos);
    this.localStorageService.setItem(DataApp.DETALLE_PEDIDOS_COMPRA_ID, detalleCompraList);
  }
}
