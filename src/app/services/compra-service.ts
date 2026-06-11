import { inject, Injectable } from '@angular/core';
import { MatSnackBar } from '@angular/material/snack-bar';
import { DetallePedido } from '../models/detalle-pedido';
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
  private _snackBar = inject(MatSnackBar);

  constructor() { }

  getDetallePedidosCompra(): Array<DetallePedido> {
    var detallePedidoList: Array<DetallePedido> = [];

    var detallePedidoListSession = this.localStorageService.getItem<Array<DetallePedido>>(DataApp.DETALLE_PEDIDOS_COMPRA_ID);
    if (detallePedidoListSession) {
      detallePedidoList = detallePedidoListSession;
    }

    return detallePedidoList;
  }

  eliminarDetallesPedidoCompra() {
    this.localStorageService.removeItem(DataApp.DETALLE_PEDIDOS_COMPRA_ID);
  }



  registrarCompra() {
    var detalles = this.getDetallePedidosCompra();
    for (const det of detalles) {
      this.registrarCompraInventario(det);
    }
    this.eliminarDetallesPedidoCompra();
  }

  registrarCompraInventario(detallePedido: DetallePedido) {

    // crear lote de compra
    let lote: LoteInventario = DataApp.loteInventarioVacio();
    lote.cantidad_actual = detallePedido.cantidad;
    lote.cantidad_inicial = detallePedido.cantidad;
    lote.costo_unitario = detallePedido.precio_unitario_venta;
    lote.fecha_ingreso = new Date();
    lote.producto = detallePedido.producto;

    lote = this.dataService.pushLoteInventario(lote);

    // guardar movimiento de inventario de compra
    let tipos = this.dataService.getTiposMovimiento();
    let movimiento: MovimientoInventario = DataApp.movimientoInventarioVacio();
    movimiento.cantidad = detallePedido.cantidad;
    movimiento.fecha = new Date();
    movimiento.producto = detallePedido.producto;
    movimiento.tipo_movimiento = tipos[tipos.findIndex(a => a.id == 1)];
    movimiento.lote = lote;

    this.dataService.pushMovimientoInventario(movimiento);

    // actualizar productos
    if (detallePedido.producto.stock_web && detallePedido.producto.stock_web > 0) {
      detallePedido.producto.stock_web -= detallePedido.cantidad;
    } else {
      detallePedido.producto.stock_web = detallePedido.cantidad;
    }

    if (detallePedido.producto.stock_local && detallePedido.producto.stock_local > 0) {
      detallePedido.producto.stock_local -= detallePedido.cantidad;
    } else {
      detallePedido.producto.stock_local = detallePedido.cantidad;
    }

    this.dataService.editarProducto(detallePedido.producto);
  }

  aumentarDetalleCompra(detalle: DetallePedido): Array<DetallePedido> {
    var detalles = this.getDetallePedidosCompra();
    for (const deta of detalles) {
      if (deta.id == detalle.id) {
        deta.cantidad++;
        this.editarDetalleCompra(deta);
      }
    }
    return detalles;
  }

  disminuirDetalleCompra(detalle: DetallePedido): Array<DetallePedido> {
    var detalles = this.getDetallePedidosCompra();
    for (const deta of detalles) {
      if (deta.id == detalle.id && deta.cantidad > 0) {
        deta.cantidad--;
        this.editarDetalleCompra(deta);
      }
    }
    return detalles;
  }

  editarDetalleCompra(datos: DetallePedido) {
    var detalleList: Array<DetallePedido> = this.getDetallePedidosCompra();
    var detalleListNueva: Array<DetallePedido> = [];

    detalleList.forEach(arrData => {
      var newData: DetallePedido = arrData;
      if (newData.id == datos.id) {
        newData = datos;
      }
      detalleListNueva.push(newData);
    });

    this.localStorageService.setItem(DataApp.DETALLE_PEDIDOS_COMPRA_ID, detalleListNueva);
  }

  pushDetalleCompra(datos: DetallePedido) {
    var detallePedidoList: Array<DetallePedido> = this.getDetallePedidosCompra();
    if (detallePedidoList.length > 0) {
      datos.id = detallePedidoList[detallePedidoList.length - 1].id + 1;
    } else {
      datos.id = 1;
    }
    detallePedidoList.push(datos);
    this.localStorageService.setItem(DataApp.DETALLE_PEDIDOS_COMPRA_ID, detallePedidoList);
  }
}
