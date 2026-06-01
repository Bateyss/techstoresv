import { inject, Injectable } from '@angular/core';
import { DetallePedido } from '../models/detalle-pedido';
import { LoteInventario } from '../models/lote-inventario';
import { MovimientoInventario } from '../models/movimiento-inventario';
import { Pedido } from '../models/pedido';
import { Producto } from '../models/producto';
import { Usuario } from '../models/usuario';
import { Utils } from '../util/utils';
import { LocalStorageService } from './local-storage-service';

@Injectable({
  providedIn: 'root'
})
export class DataService {

  readonly PRODUCTOS_ID: string = 'productoList';
  readonly USUARIOS_ID: string = 'usuarioList';
  readonly LOTES_INVENTARIO_ID: string = 'loteList';
  readonly PEDIDOS_ID: string = 'pedidoList';
  readonly DETALLE_PEDIDOS_ID: string = 'detallePedidoList';
  readonly MOVIMIENTOS_INVENTARIO_ID: string = 'movimientoInventarioList';

  private localStorageService = inject(LocalStorageService);

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

  getUsuarios(): Array<Usuario> {
    var usuarioList: Array<Usuario> = [];

    var usuarioListSession = this.localStorageService.getItem<Array<Usuario>>(this.USUARIOS_ID);
    if (usuarioListSession) {
      usuarioList = usuarioListSession;
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

    this.localStorageService.setItem(this.USUARIOS_ID, usuarioList);
    return usuarioList;
  }

  getProductos(): Array<Producto> {
    var productoList: Array<Producto> = [];

    var productoListSession = this.localStorageService.getItem<Array<Producto>>(this.PRODUCTOS_ID);
    if (productoListSession) {
      productoList = productoListSession;
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

    this.localStorageService.setItem(this.PRODUCTOS_ID, productoList);
    return productoList;
  }

  getLotesInventario(): Array<LoteInventario> {
    var loteList: Array<LoteInventario> = [];

    var loteListSession = this.localStorageService.getItem<Array<LoteInventario>>(this.LOTES_INVENTARIO_ID);
    if (loteListSession) {
      loteList = loteListSession;
    }

    var productos = this.getProductos();

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

    this.localStorageService.setItem(this.LOTES_INVENTARIO_ID, loteList);
    return loteList;
  }

  getPedidos(): Array<Pedido> {
    var pedidoList: Array<Pedido> = [];

    var pedidoListSession = this.localStorageService.getItem<Array<Pedido>>(this.PEDIDOS_ID);
    if (pedidoListSession) {
      pedidoListSession = pedidoListSession;
    }

    var usuarios = this.getUsuarios();

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

    this.localStorageService.setItem(this.PEDIDOS_ID, pedidoList);
    return pedidoList;
  }

  getDetallePedidos(): Array<DetallePedido> {
    var detallePedidoList: Array<DetallePedido> = [];

    var detallePedidoListSession = this.localStorageService.getItem<Array<DetallePedido>>(this.DETALLE_PEDIDOS_ID);
    if (detallePedidoListSession) {
      detallePedidoList = detallePedidoListSession;
    }

    var productos = this.getProductos();
    var pedidos = this.getPedidos();

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

    this.localStorageService.setItem(this.DETALLE_PEDIDOS_ID, detallePedidoList);
    return detallePedidoList;
  }

  getMovimientosInventario(): Array<MovimientoInventario> {
    var movimientoInventarioList: Array<MovimientoInventario> = [];

    var movimientoInventarioListSession = this.localStorageService.getItem<Array<MovimientoInventario>>(this.MOVIMIENTOS_INVENTARIO_ID);
    if (movimientoInventarioListSession) {
      movimientoInventarioList = movimientoInventarioListSession;
    }

    var productos = this.getProductos();
    var lotes = this.getLotesInventario();
    var pedidos = this.getPedidos();

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

    this.localStorageService.setItem(this.MOVIMIENTOS_INVENTARIO_ID, movimientoInventarioList);
    return movimientoInventarioList;
  }

  pushProducto(datos: Producto) {
    var productoList: Array<Producto> = this.getProductos();
    datos.id = productoList[productoList.length - 1].id + 1;
    productoList.push(datos);
    this.localStorageService.setItem(this.PRODUCTOS_ID, productoList);
  }
  pushLoteInventario(datos: LoteInventario) {

    var loteList: Array<LoteInventario> = this.getLotesInventario();
    datos.id = loteList[loteList.length - 1].id + 1;
    loteList.push(datos);
    this.localStorageService.setItem(this.LOTES_INVENTARIO_ID, loteList);
  }

  pushPedido(datos: Pedido) {
    var pedidoList: Array<Pedido> = this.getPedidos();
    datos.id = pedidoList[pedidoList.length - 1].id + 1;
    pedidoList.push(datos);
    this.localStorageService.setItem(this.PEDIDOS_ID, pedidoList);
  }

  pushDetallePedido(datos: DetallePedido) {
    var detallePedidoList: Array<DetallePedido> = this.getDetallePedidos();
    datos.id = detallePedidoList[detallePedidoList.length - 1].id + 1;
    detallePedidoList.push(datos);
    this.localStorageService.setItem(this.DETALLE_PEDIDOS_ID, detallePedidoList);
  }

  pushMovimientoInventario(datos: MovimientoInventario) {
    var movimientoInventarioList: Array<MovimientoInventario> = this.getMovimientosInventario();
    datos.id = movimientoInventarioList[movimientoInventarioList.length - 1].id + 1;
    movimientoInventarioList.push(datos);
    this.localStorageService.setItem(this.MOVIMIENTOS_INVENTARIO_ID, movimientoInventarioList);
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

    this.localStorageService.setItem(this.PRODUCTOS_ID, productoListNueva);
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

    this.localStorageService.setItem(this.LOTES_INVENTARIO_ID, loteListNueva);
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

    this.localStorageService.setItem(this.DETALLE_PEDIDOS_ID, detalleListNueva);
  }



}
