import { inject, Injectable } from '@angular/core';
import { DetallePedido } from '../models/detalle-pedido';
import { EstadoPedido } from '../models/estado-pedido';
import { EstadoProducto } from '../models/estado-producto';
import { LoteInventario } from '../models/lote-inventario';
import { MovimientoInventario } from '../models/movimiento-inventario';
import { Pasarela } from '../models/pasarela';
import { Pedido } from '../models/pedido';
import { Producto } from '../models/producto';
import { TipoMovimiento } from '../models/tipo-movimiento';
import { Usuario } from '../models/usuario';
import { DataApp } from '../util/data-app';
import { LocalStorageService } from './local-storage-service';

@Injectable({
  providedIn: 'root'
})
export class DataService {

  private localStorageService = inject(LocalStorageService);

  constructor() { }

  getUsuarios(): Array<Usuario> {
    var usuarioList: Array<Usuario> = [];

    var usuarioListSession = this.localStorageService.getItem<Array<Usuario>>(DataApp.USUARIOS_ID);
    if (usuarioListSession) {
      usuarioList = usuarioListSession;
    }

    if (!usuarioList || usuarioList.length == 0) {
      usuarioList = DataApp.getUsuarios();
      this.localStorageService.setItem(DataApp.USUARIOS_ID, usuarioList);
    }

    return usuarioList;
  }

  getEstadosPedido(): Array<EstadoPedido> {
    var estadoPedidoList: Array<EstadoPedido> = [];

    var estadoPedidoListSession = this.localStorageService.getItem<Array<EstadoPedido>>(DataApp.ESTADO_PEDIDO_ID);
    if (estadoPedidoListSession) {
      estadoPedidoList = estadoPedidoListSession;
    }

    if (!estadoPedidoList || estadoPedidoList.length == 0) {
      estadoPedidoList = DataApp.getEstadosPedido();
      this.localStorageService.setItem(DataApp.ESTADO_PEDIDO_ID, estadoPedidoList);
    }

    return estadoPedidoList;
  }

  getEstadosProducto(): Array<EstadoProducto> {
    var estadoProductoList: Array<EstadoProducto> = [];

    var estadoPedidoListSession = this.localStorageService.getItem<Array<EstadoProducto>>(DataApp.ESTADO_PRODUCTO_ID);
    if (estadoPedidoListSession) {
      estadoProductoList = estadoPedidoListSession;
    }

    if (!estadoProductoList || estadoProductoList.length == 0) {
      estadoProductoList = DataApp.getEstadosProducto();
      this.localStorageService.setItem(DataApp.ESTADO_PRODUCTO_ID, estadoProductoList);
    }

    return estadoProductoList;
  }

  getTiposMovimiento(): Array<TipoMovimiento> {
    var tipoMovimientoList: Array<TipoMovimiento> = [];

    var tipoMovimientoListSession = this.localStorageService.getItem<Array<TipoMovimiento>>(DataApp.TIPO_MOVIMIENTO_ID);
    if (tipoMovimientoListSession) {
      tipoMovimientoList = tipoMovimientoListSession;
    }

    if (!tipoMovimientoList || tipoMovimientoList.length == 0) {
      tipoMovimientoList = DataApp.getTiposMovimiento();
      this.localStorageService.setItem(DataApp.TIPO_MOVIMIENTO_ID, tipoMovimientoList);
    }

    return tipoMovimientoList;
  }

  getProductos(): Array<Producto> {
    var productoList: Array<Producto> = [];

    var productoListSession = this.localStorageService.getItem<Array<Producto>>(DataApp.PRODUCTOS_ID);
    if (productoListSession) {
      productoList = productoListSession;
    }

    if (!productoList || productoList.length == 0) {
      productoList = DataApp.getProductos();
      this.localStorageService.setItem(DataApp.PRODUCTOS_ID, productoList);
    }

    return productoList;
  }

  getLotesInventario(): Array<LoteInventario> {
    var loteList: Array<LoteInventario> = [];

    var loteListSession = this.localStorageService.getItem<Array<LoteInventario>>(DataApp.LOTES_INVENTARIO_ID);
    if (loteListSession) {
      loteList = loteListSession;
    }

    if (!loteList || loteList.length == 0) {
      loteList = DataApp.getLotesInventario();
      this.localStorageService.setItem(DataApp.LOTES_INVENTARIO_ID, loteList);
    }

    return loteList;
  }

  getPedidos(): Array<Pedido> {
    var pedidoList: Array<Pedido> = [];

    var pedidoListSession = this.localStorageService.getItem<Array<Pedido>>(DataApp.PEDIDOS_ID);
    if (pedidoListSession) {
      pedidoList = pedidoListSession;
    }

    if (!pedidoList || pedidoList.length == 0) {
      pedidoList = DataApp.getPedidos();
      this.localStorageService.setItem(DataApp.PEDIDOS_ID, pedidoList);
    }

    return pedidoList;
  }

  getDetallePedidos(): Array<DetallePedido> {
    var detallePedidoList: Array<DetallePedido> = [];

    var detallePedidoListSession = this.localStorageService.getItem<Array<DetallePedido>>(DataApp.DETALLE_PEDIDOS_ID);
    if (detallePedidoListSession) {
      detallePedidoList = detallePedidoListSession;
    }

    if (!detallePedidoList || detallePedidoList.length == 0) {
      detallePedidoList = DataApp.getDetallePedidos();
      this.localStorageService.setItem(DataApp.DETALLE_PEDIDOS_ID, detallePedidoList);
    }

    return detallePedidoList;
  }

  getMovimientosInventario(): Array<MovimientoInventario> {
    var movimientoInventarioList: Array<MovimientoInventario> = [];

    var movimientoInventarioListSession = this.localStorageService.getItem<Array<MovimientoInventario>>(DataApp.MOVIMIENTOS_INVENTARIO_ID);
    if (movimientoInventarioListSession) {
      movimientoInventarioList = movimientoInventarioListSession;
    }

    if (!movimientoInventarioList || movimientoInventarioList.length == 0) {
      movimientoInventarioList = DataApp.getMovimientosInventario();
      this.localStorageService.setItem(DataApp.MOVIMIENTOS_INVENTARIO_ID, movimientoInventarioList);
    }

    return movimientoInventarioList;
  }

  getPasarelas(): Array<Pasarela> {
    var pasarelaList: Array<Pasarela> = [];

    var pasarelaListSession = this.localStorageService.getItem<Array<Pasarela>>(DataApp.PASARELA_ID);
    if (pasarelaListSession) {
      pasarelaList = pasarelaListSession;
      this.localStorageService.setItem(DataApp.PASARELA_ID, pasarelaList);
    }

    if (!pasarelaList || pasarelaList.length == 0) {
      pasarelaList = DataApp.getPasarelas();
    }

    return pasarelaList;
  }

  pushProducto(datos: Producto) {
    var productoList: Array<Producto> = this.getProductos();
    datos.id = productoList[productoList.length - 1].id + 1;
    productoList.push(datos);
    this.localStorageService.setItem(DataApp.PRODUCTOS_ID, productoList);
  }
  pushLoteInventario(datos: LoteInventario) {

    var loteList: Array<LoteInventario> = this.getLotesInventario();
    datos.id = loteList[loteList.length - 1].id + 1;
    loteList.push(datos);
    this.localStorageService.setItem(DataApp.LOTES_INVENTARIO_ID, loteList);
    return datos;
  }

  pushPedido(datos: Pedido): Pedido {
    var pedidoList: Array<Pedido> = this.getPedidos();
    datos.id = pedidoList[pedidoList.length - 1].id + 1;
    pedidoList.push(datos);
    this.localStorageService.setItem(DataApp.PEDIDOS_ID, pedidoList);
    return datos;
  }

  pushDetallePedido(datos: DetallePedido) {
    var detallePedidoList: Array<DetallePedido> = this.getDetallePedidos();
    datos.id = detallePedidoList[detallePedidoList.length - 1].id + 1;
    detallePedidoList.push(datos);
    this.localStorageService.setItem(DataApp.DETALLE_PEDIDOS_ID, detallePedidoList);
  }

  pushMovimientoInventario(datos: MovimientoInventario) {
    var movimientoInventarioList: Array<MovimientoInventario> = this.getMovimientosInventario();
    datos.id = movimientoInventarioList[movimientoInventarioList.length - 1].id + 1;
    movimientoInventarioList.push(datos);
    this.localStorageService.setItem(DataApp.MOVIMIENTOS_INVENTARIO_ID, movimientoInventarioList);
  }

  editarProducto(datos: Producto) {
    var productoList: Array<Producto> = this.getProductos();
    var productoListNueva: Array<Producto> = [];

    productoList.forEach(arrData => {
      var newData: Producto = arrData;
      if (newData.id == datos.id) {
        newData = datos;
      }
      productoListNueva.push(newData);
    });

    this.localStorageService.setItem(DataApp.PRODUCTOS_ID, productoListNueva);
  }

  editarLoteInventario(datos: LoteInventario) {
    var loteList: Array<LoteInventario> = this.getLotesInventario();
    var loteListNueva: Array<LoteInventario> = [];

    loteList.forEach(arrData => {
      var newData: LoteInventario = arrData;
      if (newData.id == datos.id) {
        newData = datos;
      }
      loteListNueva.push(newData);
    });

    this.localStorageService.setItem(DataApp.LOTES_INVENTARIO_ID, loteListNueva);
  }

  editarDetallePedido(datos: DetallePedido) {
    var detalleList: Array<DetallePedido> = this.getDetallePedidos();
    var detalleListNueva: Array<DetallePedido> = [];

    detalleList.forEach(arrData => {
      var newData: DetallePedido = arrData;
      if (newData.id == datos.id) {
        newData = datos;
      }
      detalleListNueva.push(newData);
    });

    this.localStorageService.setItem(DataApp.DETALLE_PEDIDOS_ID, detalleListNueva);
  }

  editarPedido(datos: Pedido) {
    var pedidoList: Array<Pedido> = this.getPedidos();
    var pedidoListNueva: Array<Pedido> = [];
    pedidoList.forEach(arrData => {
      var newData: Pedido = arrData;
      if (newData.id == datos.id) {
        newData = datos;
      }
      pedidoListNueva.push(newData);
    });
    this.localStorageService.setItem(DataApp.PEDIDOS_ID, pedidoListNueva);
  }



}
