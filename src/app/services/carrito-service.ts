import { inject, Injectable } from '@angular/core';
import { DetallePedido } from '../models/detalle-pedido';
import { Pedido } from '../models/pedido';
import { Usuario } from '../models/usuario';
import { DataApp } from '../util/data-app';
import { DataService } from './data.service';
import { MovimientoInventario } from '../models/movimiento-inventario';
import { MatSnackBar } from '@angular/material/snack-bar';
import { LoteInventario } from '../models/lote-inventario';

@Injectable({
  providedIn: 'root',
})
export class CarritoService {

  private dataService = inject(DataService);
  private _snackBar = inject(MatSnackBar);

  constructor() { }

  getPedidosUsuario(usuario: Usuario): Array<Pedido> {
    var pedidosDeUsuario = [];
    var pedidos = this.dataService.getPedidos();
    if (pedidos.length > 0) {
      for (const ped of pedidos) {
        if (ped.usuario.id == usuario.id && ped.estado.id < 3) pedidosDeUsuario.push(ped);
      }
    }
    return pedidosDeUsuario;
  }

  getUltimoPedidoUsuario(usuario: Usuario): Pedido {
    var pedido = DataApp.pedidoVacio();
    var pedidos = this.getPedidosUsuario(usuario)
    if (pedidos.length > 0) {
      pedido = pedidos[pedidos.length - 1];
    }
    return pedido;
  }

  getDetallesPedido(pedido: Pedido): Array<DetallePedido> {
    var detallesDePedido = [];
    var detalles = this.dataService.getDetallePedidos();
    if (detalles.length > 0) {
      for (const detalle of detalles) {
        if (detalle.pedido.id == pedido.id) detallesDePedido.push(detalle);
      }
    }
    return detallesDePedido;
  }

  aumentarDetallePedido(detalle: DetallePedido): Array<DetallePedido> {
    var detalles = this.getDetallesPedido(detalle.pedido);
    for (const deta of detalles) {
      if (deta.id == detalle.id) {
        deta.cantidad++;
        this.dataService.editarDetallePedido(deta);
      }
    }
    return detalles;
  }

  disminuirDetallePedido(detalle: DetallePedido): Array<DetallePedido> {
    var detalles = this.getDetallesPedido(detalle.pedido);
    for (const deta of detalles) {
      if (deta.id == detalle.id && deta.cantidad > 0) {
        deta.cantidad--;
        this.dataService.editarDetallePedido(deta);
      }
    }
    return detalles;
  }

  pedidoPagado(pedido: Pedido) {
    var estados = this.dataService.getEstadosPedido();
    pedido.estado = estados[2];
    this.dataService.editarPedido(pedido);

    var detalles = this.getDetallesPedido(pedido);
    for (const det of detalles) {
      if (det.pedido.id = pedido.id) {
        det.pedido = pedido;
        this.dataService.editarDetallePedido(det);

        this.registrarPagoInventario(det);
      }
    }
  }

  registrarPagoInventario(detallePedido: DetallePedido) {
    let movimiento: MovimientoInventario = DataApp.movimientoInventarioVacio();

    movimiento.cantidad = detallePedido.cantidad;
    movimiento.fecha = new Date();
    movimiento.pedido = detallePedido.pedido;
    movimiento.producto = detallePedido.producto;

    let tipos = this.dataService.getTiposMovimiento();
    movimiento.tipo_movimiento = tipos[tipos.findIndex(a => a.id == 2)];

    let lotes = this.dataService.getLotesInventario();
    let lotesDos: Array<LoteInventario> = [];

    let cantidadEstimada = 0;
    cantidadEstimada = detallePedido.cantidad;
    for (const lote of lotes) {
      if (lote.producto.id == detallePedido.producto.id && lote.cantidad_actual > 0 && cantidadEstimada > 0) {
        lotesDos.push(lote);
        if (lote.cantidad_actual < cantidadEstimada) {
          cantidadEstimada -= lote.cantidad_actual;
        }
      }
    }

    // guardar movimiento y actualizar lote
    cantidadEstimada = detallePedido.cantidad;
    for (const lote of lotesDos) {
      if (cantidadEstimada > 0) {
        if (lote.cantidad_actual < cantidadEstimada) {
          cantidadEstimada -= lote.cantidad_actual;
          lote.cantidad_actual = 0;
        } else {
          lote.cantidad_actual -= cantidadEstimada;
          cantidadEstimada = 0;
        }
        let movimientoDos = movimiento;
        movimientoDos.lote = lote;
        this.dataService.editarLoteInventario(lote);
        this.dataService.pushMovimientoInventario(movimientoDos);
      }
    }

    // actualizar productos
    if (detallePedido.producto.stock_web && detallePedido.producto.stock_web > 0) {
      detallePedido.producto.stock_web -= detallePedido.cantidad;

      this.dataService.editarProducto(detallePedido.producto);
    }

  }

  validarExistenciaInventario(detallePedido: DetallePedido): boolean {

    let lotes = this.dataService.getLotesInventario();
    let lotesDos: Array<LoteInventario> = [];

    let cantidadEstimada = 0;
    cantidadEstimada = detallePedido.cantidad;
    for (const lote of lotes) {
      if (lote.producto.id == detallePedido.producto.id && lote.cantidad_actual > 0 && cantidadEstimada > 0) {
        lotesDos.push(lote);
        if (lote.cantidad_actual < cantidadEstimada) {
          cantidadEstimada -= lote.cantidad_actual;
        } else {
          cantidadEstimada = 0;
        }
      }
    }
    return (lotesDos && lotesDos.length > 0 && cantidadEstimada == 0);
  }

}
