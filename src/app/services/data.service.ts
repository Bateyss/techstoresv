import { Injectable } from '@angular/core';
import { Producto } from '../models/producto';
import { LoteInventario } from '../models/lote-inventario';
import { Pedido } from '../models/pedido';
import { DetallePedido } from '../models/detalle-pedido';
import { MovimientoInventario } from '../models/movimiento-inventario';
import { Usuario } from '../models/usuario';
import { Utils } from '../util/utils';

@Injectable({
  providedIn: 'root'
})
export class DataService {

  private PRODUCTOS_ID: string = 'productoList';
  private USUARIOS_ID: string = 'usuarioList';
  private LOTES_INVENTARIO_ID: string = 'loteList';
  private PEDIDOS_ID: string = 'pedidoList';
  private DETALLE_PEDIDOS_ID: string = 'detallePedidoList';
  private MOVIMIENTOS_INVENTARIO_ID: string = 'movimientoInventarioList';

  constructor() { }

  usuarioVacio(): Usuario {
    return {
      id: 0,
      usuario: '',
      password: '',
      nombres: '',
      apellidos: '',
      fecha_creacion: new Date()
    }
  }

  productoVacio(): Producto {
    return {
      id: 0,
      sku: '',
      nombre: '',
      descripcion: '',
      precio_venta: 0,
      estado: 0
    }
  }

  loteInventarioVacio(): LoteInventario {
    return {
      id: 0,
      producto: this.productoVacio(),
      cantidad_inicial: 0,
      cantidad_actual: 0,
      costo_unitario: 0,
      fecha_ingreso: new Date()
    }
  }

  pedidoVacio(): Pedido {
    return {
      id: 0,
      usuario: this.usuarioVacio(),
      total: 0,
      estado: 0
    }
  }

  detallePedidoVacio(): DetallePedido {
    return {
      id: 0,
      pedido: this.pedidoVacio(),
      producto: this.productoVacio(),
      cantidad: 0,
      precio_unitario_venta: 0
    }
  }

  movimientoInventarioVacio(): MovimientoInventario {
    return {
      id: 0,
      // se mantendra producto en redundancia con lote
      // para consultas rapidas por producto y evitar joins
      producto: this.productoVacio(),
      lote: this.loteInventarioVacio(),
      tipo_movimiento: 0,
      cantidad: 0,
      pedido: this.pedidoVacio(),
      fecha: new Date
    }
  }

  getUsuarios(document: Document): Array<Usuario> {
    var localStorage = document.defaultView?.localStorage;
    var usuarioList: Array<Usuario> = [];

    var usuarioListSession = localStorage?.getItem(this.USUARIOS_ID);
    if (usuarioListSession) {
      usuarioList = JSON.parse(usuarioListSession) as Array<Usuario>;
    }

    var genericPass = '';
    Utils.generateSHA256('1234').then(b => genericPass = b);

    if (!usuarioList || usuarioList.length == 0) {
      usuarioList.push({
        id: 1,
        usuario: 'anonim',
        password: '',
        nombres: 'anonimo',
        apellidos: '',
        fecha_creacion: new Date()
      });
      usuarioList.push({
        id: 2,
        usuario: 'jamileth',
        password: genericPass,
        nombres: 'Jamileth',
        apellidos: 'Martinez',
        fecha_creacion: new Date()
      });
      usuarioList.push({
        id: 3,
        usuario: 'rubix',
        password: genericPass,
        nombres: 'Rubi',
        apellidos: 'Mejia',
        fecha_creacion: new Date()
      });
      usuarioList.push({
        id: 4,
        usuario: 'ale',
        password: genericPass,
        nombres: 'Alejandra',
        apellidos: 'Guardado',
        fecha_creacion: new Date()
      });
      usuarioList.push({
        id: 5,
        usuario: 'khaysernberg',
        password: genericPass,
        nombres: 'Cesar',
        apellidos: 'Gomez',
        fecha_creacion: new Date()
      });
      usuarioList.push({
        id: 6,
        usuario: 'miriam',
        password: genericPass,
        nombres: 'Mirian',
        apellidos: 'Rivas',
        fecha_creacion: new Date()
      });
    }

    localStorage?.setItem(this.USUARIOS_ID, JSON.stringify(usuarioList));
    return usuarioList;
  }

  getUsuarioAnonimo(): Usuario {
    return {
      id: 1,
      usuario: 'anonim',
      password: '',
      nombres: 'anonimo',
      apellidos: '',
      fecha_creacion: new Date()
    }
  }

  getProductos(document: Document): Array<Producto> {
    var localStorage = document.defaultView?.localStorage;
    var productoList: Array<Producto> = [];

    var productoListSession = localStorage?.getItem(this.PRODUCTOS_ID);
    if (productoListSession) {
      productoList = JSON.parse(productoListSession) as Array<Producto>;
    }

    if (!productoList || productoList.length == 0) {
      productoList.push({
        id: 1,
        sku: 'p1',
        nombre: 'producto1',
        descripcion: 'descripcion1',
        precio_venta: 1.50,
        estado: 1
      });
      productoList.push({
        id: 2,
        sku: 'p2',
        nombre: 'producto2',
        descripcion: 'descripcion2',
        precio_venta: 2.50,
        estado: 1
      });
      productoList.push({
        id: 3,
        sku: 'p3',
        nombre: 'producto3',
        descripcion: 'descripcion3',
        precio_venta: 3.50,
        estado: 1
      });
    }

    localStorage?.setItem(this.PRODUCTOS_ID, JSON.stringify(productoList));
    return productoList;

  }

  getLotesInventario(document: Document): Array<LoteInventario> {
    var localStorage = document.defaultView?.localStorage;
    var loteList: Array<LoteInventario> = [];

    var loteListSession = localStorage?.getItem(this.LOTES_INVENTARIO_ID);
    if (loteListSession) {
      loteList = JSON.parse(loteListSession) as Array<LoteInventario>;
    }

    var productos = this.getProductos(document);

    if (!loteList || loteList.length == 0) {
      loteList.push({
        id: 1,
        producto: productos[0],
        cantidad_inicial: 100,
        cantidad_actual: 100,
        costo_unitario: 0.9,
        fecha_ingreso: new Date()
      });
      loteList.push({
        id: 2,
        producto: productos[0],
        cantidad_inicial: 50,
        cantidad_actual: 50,
        costo_unitario: 0.95,
        fecha_ingreso: new Date()
      });
      loteList.push({
        id: 3,
        producto: productos[1],
        cantidad_inicial: 75,
        cantidad_actual: 75,
        costo_unitario: 2,
        fecha_ingreso: new Date()
      });
      loteList.push({
        id: 4,
        producto: productos[2],
        cantidad_inicial: 150,
        cantidad_actual: 150,
        costo_unitario: 2.75,
        fecha_ingreso: new Date()
      });
    }

    localStorage?.setItem(this.LOTES_INVENTARIO_ID, JSON.stringify(loteList));
    return loteList;

  }

  getPedidos(document: Document): Array<Pedido> {
    var localStorage = document.defaultView?.localStorage;
    var pedidoList: Array<Pedido> = [];

    var pedidoListSession = localStorage?.getItem(this.PEDIDOS_ID);
    if (pedidoListSession) {
      pedidoList = JSON.parse(pedidoListSession) as Array<Pedido>;
    }

    var usuarios = this.getUsuarios(document);

    if (!pedidoList || pedidoList.length == 0) {
      pedidoList.push({
        id: 1,
        usuario: usuarios[1],
        total: 10,
        estado: 1
      });
      pedidoList.push({
        id: 2,
        usuario: usuarios[2],
        total: 10,
        estado: 1
      });
      pedidoList.push({
        id: 3,
        usuario: usuarios[3],
        total: 10,
        estado: 1
      });
    }

    localStorage?.setItem(this.PEDIDOS_ID, JSON.stringify(pedidoList));
    return pedidoList;
  }

  getDetallePedidos(document: Document): Array<DetallePedido> {
    var localStorage = document.defaultView?.localStorage;
    var detallePedidoList: Array<DetallePedido> = [];

    var detallePedidoListSession = localStorage?.getItem(this.DETALLE_PEDIDOS_ID);
    if (detallePedidoListSession) {
      detallePedidoList = JSON.parse(detallePedidoListSession) as Array<DetallePedido>;
    }

    var productos = this.getProductos(document);
    var pedidos = this.getPedidos(document);

    if (!detallePedidoList || detallePedidoList.length == 0) {
      detallePedidoList.push({
        id: 1,
        pedido: pedidos[0],
        producto: productos[0],
        cantidad: 2,
        precio_unitario_venta: productos[0].precio_venta
      });
      detallePedidoList.push({
        id: 2,
        pedido: pedidos[0],
        producto: productos[2],
        cantidad: 2,
        precio_unitario_venta: productos[2].precio_venta
      });
      detallePedidoList.push({
        id: 3,
        pedido: pedidos[1],
        producto: productos[0],
        cantidad: 2,
        precio_unitario_venta: productos[0].precio_venta
      });
      detallePedidoList.push({
        id: 4,
        pedido: pedidos[1],
        producto: productos[2],
        cantidad: 2,
        precio_unitario_venta: productos[2].precio_venta
      });
      detallePedidoList.push({
        id: 5,
        pedido: pedidos[2],
        producto: productos[0],
        cantidad: 2,
        precio_unitario_venta: productos[0].precio_venta
      });
      detallePedidoList.push({
        id: 6,
        pedido: pedidos[2],
        producto: productos[2],
        cantidad: 2,
        precio_unitario_venta: productos[2].precio_venta
      });
    }

    localStorage?.setItem(this.DETALLE_PEDIDOS_ID, JSON.stringify(detallePedidoList));
    return detallePedidoList;
  }

  getMovimientosInventario(document: Document): Array<MovimientoInventario> {
    var localStorage = document.defaultView?.localStorage;
    var movimientoInventarioList: Array<MovimientoInventario> = [];

    var movimientoInventarioListSession = localStorage?.getItem(this.MOVIMIENTOS_INVENTARIO_ID);
    if (movimientoInventarioListSession) {
      movimientoInventarioList = JSON.parse(movimientoInventarioListSession) as Array<MovimientoInventario>;
    }

    var productos = this.getProductos(document);
    var lotes = this.getLotesInventario(document);
    var pedidos = this.getPedidos(document);

    if (!movimientoInventarioList || movimientoInventarioList.length == 0) {
      movimientoInventarioList.push({
        id: 1,
        producto: productos[0],
        lote: lotes[0],
        tipo_movimiento: 1,
        cantidad: 10,
        pedido: pedidos[0],
        fecha: new Date()
      });
      movimientoInventarioList.push({
        id: 1,
        producto: productos[2],
        lote: lotes[2],
        tipo_movimiento: 1,
        cantidad: 10,
        pedido: pedidos[0],
        fecha: new Date()
      });
    }

    localStorage?.setItem(this.MOVIMIENTOS_INVENTARIO_ID, JSON.stringify(movimientoInventarioList));
    return movimientoInventarioList;
  }

  pushProducto(document: Document, datos: Producto) {
    var localStorage = document.defaultView?.localStorage;
    var productoList: Array<Producto> = this.getProductos(document);
    datos.id = productoList[productoList.length - 1].id + 1;
    productoList.push(datos);
    localStorage?.setItem(this.PRODUCTOS_ID, JSON.stringify(productoList));
  }

  pushLoteInventario(document: Document, datos: LoteInventario) {
    var localStorage = document.defaultView?.localStorage;
    var loteList: Array<LoteInventario> = this.getLotesInventario(document);
    datos.id = loteList[loteList.length - 1].id + 1;
    loteList.push(datos);
    localStorage?.setItem(this.LOTES_INVENTARIO_ID, JSON.stringify(loteList));
  }

  pushPedido(document: Document, datos: Pedido) {
    var localStorage = document.defaultView?.localStorage;
    var pedidoList: Array<Pedido> = this.getPedidos(document);
    datos.id = pedidoList[pedidoList.length - 1].id + 1;
    pedidoList.push(datos);
    localStorage?.setItem(this.PEDIDOS_ID, JSON.stringify(pedidoList));
  }

  pushDetallePedido(document: Document, datos: DetallePedido) {
    var localStorage = document.defaultView?.localStorage;
    var detallePedidoList: Array<DetallePedido> = this.getDetallePedidos(document);
    datos.id = detallePedidoList[detallePedidoList.length - 1].id + 1;
    detallePedidoList.push(datos);
    localStorage?.setItem(this.DETALLE_PEDIDOS_ID, JSON.stringify(detallePedidoList));
  }

  pushMovimientoInventario(document: Document, datos: MovimientoInventario) {
    var localStorage = document.defaultView?.localStorage;
    var movimientoInventarioList: Array<MovimientoInventario> = this.getMovimientosInventario(document);
    datos.id = movimientoInventarioList[movimientoInventarioList.length - 1].id + 1;
    movimientoInventarioList.push(datos);
    localStorage?.setItem(this.MOVIMIENTOS_INVENTARIO_ID, JSON.stringify(movimientoInventarioList));
  }

  editarProducto(document: Document, datos: Producto) {
    var productoList: Array<Producto> = this.getProductos(document);
    var productoList2: Array<Producto> = [];

    productoList.forEach(arrData => {
      var newData: Producto = arrData;
      if (newData.id == datos.id) {
        newData = datos;
      }
      productoList2.push(newData);
    });

    localStorage?.setItem(this.PRODUCTOS_ID, JSON.stringify(productoList2));
  }

  editarLoteInventario(document: Document, datos: LoteInventario) {
    var loteList: Array<LoteInventario> = this.getLotesInventario(document);
    var loteList2: Array<LoteInventario> = [];

    loteList.forEach(arrData => {
      var newData: LoteInventario = arrData;
      if (newData.id == datos.id) {
        newData = datos;
      }
      loteList2.push(newData);
    });

    localStorage?.setItem(this.LOTES_INVENTARIO_ID, JSON.stringify(loteList2));
  }

  editarDetallePedido(document: Document, datos: DetallePedido) {
    var detalleList: Array<DetallePedido> = this.getDetallePedidos(document);
    var detalleList2: Array<DetallePedido> = [];

    detalleList.forEach(arrData => {
      var newData: DetallePedido = arrData;
      if (newData.id == datos.id) {
        newData = datos;
      }
      detalleList2.push(newData);
    });

    localStorage?.setItem(this.DETALLE_PEDIDOS_ID, JSON.stringify(detalleList2));
  }



}
