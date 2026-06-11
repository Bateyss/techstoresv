import { DetalleCompra } from "../models/detalle-compra";
import { DetallePedido } from "../models/detalle-pedido";
import { EstadoPedido } from "../models/estado-pedido";
import { EstadoProducto } from "../models/estado-producto";
import { LoteInventario } from "../models/lote-inventario";
import { MovimientoInventario } from "../models/movimiento-inventario";
import { Pasarela } from "../models/pasarela";
import { Pedido } from "../models/pedido";
import { Producto } from "../models/producto";
import { TipoMovimiento } from "../models/tipo-movimiento";
import { Usuario } from "../models/usuario";
import { Utils } from "./utils";

export class DataApp {

    public static readonly PRODUCTOS_ID: string = 'productoList';
    public static readonly USUARIOS_ID: string = 'usuarioList';
    public static readonly LOTES_INVENTARIO_ID: string = 'loteList';
    public static readonly PEDIDOS_ID: string = 'pedidoList';
    public static readonly DETALLE_PEDIDOS_ID: string = 'detallePedidoList';
    public static readonly MOVIMIENTOS_INVENTARIO_ID: string = 'movimientoInventarioList';
    public static readonly ESTADO_PEDIDO_ID: string = 'estadoPedidoList';
    public static readonly ESTADO_PRODUCTO_ID: string = 'estadoProductoList';
    public static readonly TIPO_MOVIMIENTO_ID: string = 'tipoMovimientoList';

    public static readonly PASARELA_ID: string = 'pasarelaList';
    public static readonly DETALLE_PEDIDOS_COMPRA_ID: string = 'detallePedidoCompraList';

    public static readonly LOGGED_USUARIO: string = 'usuario';
    public static readonly LOGGED: string = 'logged';

    public static estadoPedidoVacio(): EstadoPedido {
        return {
            id: 0,
            descripcion: ''
        }
    }

    public static estadoProductoVacio(): EstadoProducto {
        return {
            id: 0,
            descripcion: ''
        }
    }

    public static tipoMovimientoVacio(): TipoMovimiento {
        return {
            id: 0,
            descripcion: '',
            cuenta: ''
        }
    }

    public static usuarioVacio(): Usuario {
        return {
            id: 0,
            usuario: '',
            password: '',
            nombres: '',
            apellidos: '',
            fecha_creacion: new Date()
        }
    }

    public static productoVacio(): Producto {
        return {
            id: 0,
            sku: '',
            nombre: '',
            descripcion: '',
            precio_venta: 0,
            estado: this.estadoProductoVacio()
        }
    }

    public static loteInventarioVacio(): LoteInventario {
        return {
            id: 0,
            producto: this.productoVacio(),
            cantidad_inicial: 0,
            cantidad_actual: 0,
            costo_unitario: 0,
            fecha_ingreso: new Date()
        }
    }

    public static pedidoVacio(): Pedido {
        return {
            id: 0,
            usuario: this.usuarioVacio(),
            total: 0,
            estado: this.estadoPedidoVacio()
        }
    }

    public static detallePedidoVacio(): DetallePedido {
        return {
            id: 0,
            pedido: this.pedidoVacio(),
            producto: this.productoVacio(),
            cantidad: 0,
            precio_unitario_venta: 0
        }
    }

    public static detalleCompraVacio(): DetalleCompra {
        return {
            id: 0,
            producto: this.productoVacio(),
            cantidad: 0,
            cantidad_local: 0,
            cantidad_web: 0,
            precio_unitario_venta: 0
        }
    }

    public static movimientoInventarioVacio(): MovimientoInventario {
        return {
            id: 0,
            // se mantendra producto en redundancia con lote
            // para consultas rapidas por producto y evitar joins
            producto: this.productoVacio(),
            lote: this.loteInventarioVacio(),
            tipo_movimiento: this.tipoMovimientoVacio(),
            cantidad: 0,
            pedido: this.pedidoVacio(),
            fecha: new Date
        }
    }

    public static pasarelaVacio(): Pasarela {
        return {
            id: 0,
            nombre: '',
            descripcion: '',
            comision: 0
        }
    }


    public static getUsuarios(): Array<Usuario> {
        var usuarioList: Array<Usuario> = [];
        var genericPass = '';
        Utils.generateSHA256('1234').then(b => genericPass = b);
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
        return usuarioList;
    }

    public static getEstadosPedido(): Array<EstadoPedido> {
        var estadoPedidoList: Array<EstadoPedido> = [];
        estadoPedidoList.push({
            id: 1,
            descripcion: 'Carrito'
        });
        estadoPedidoList.push({
            id: 2,
            descripcion: 'Apartado'
        });
        estadoPedidoList.push({
            id: 3,
            descripcion: 'Pagado'
        });
        return estadoPedidoList;
    }

    public static getEstadosProducto(): Array<EstadoProducto> {
        var estadoProductoList: Array<EstadoProducto> = [];
        estadoProductoList.push({
            id: 1,
            descripcion: 'Disponible'
        });
        estadoProductoList.push({
            id: 2,
            descripcion: 'NoDisponible'
        });
        estadoProductoList.push({
            id: 3,
            descripcion: 'Agotado'
        });
        return estadoProductoList;
    }

    public static getTiposMovimiento(): Array<TipoMovimiento> {
        var tipoMovimientoList: Array<TipoMovimiento> = [];
        tipoMovimientoList.push({
            id: 1,
            descripcion: 'Compra',
            cuenta: '410101'
        });
        tipoMovimientoList.push({
            id: 2,
            descripcion: 'Venta',
            cuenta: '210801'
        });
        tipoMovimientoList.push({
            id: 3,
            descripcion: 'Merma',
            cuenta: '420201'
        });
        tipoMovimientoList.push({
            id: 4,
            descripcion: 'Devolucion Compra',
            cuenta: '520403'
        });
        tipoMovimientoList.push({
            id: 5,
            descripcion: 'Devolucion Venta',
            cuenta: '510103'
        });
        return tipoMovimientoList;
    }

    public static getProductos(): Array<Producto> {
        var productoList: Array<Producto> = [];
        var estadosProductos = this.getEstadosProducto();
        productoList.push({
            id: 1,
            sku: 'p1',
            nombre: 'producto1',
            descripcion: 'descripcion1',
            precio_venta: 1.50,
            estado: estadosProductos[0],
            stock_local: 25,
            stock_web: 125
        });
        productoList.push({
            id: 2,
            sku: 'p2',
            nombre: 'producto2',
            descripcion: 'descripcion2',
            precio_venta: 2.50,
            estado: estadosProductos[0],
            stock_local: 25,
            stock_web: 50
        });
        productoList.push({
            id: 3,
            sku: 'p3',
            nombre: 'producto3',
            descripcion: 'descripcion3',
            precio_venta: 3.50,
            estado: estadosProductos[0],
            stock_local: 25,
            stock_web: 125
        });
        return productoList;
    }

    public static getLotesInventario(): Array<LoteInventario> {
        var loteList: Array<LoteInventario> = [];
        var productos = this.getProductos();
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
        return loteList;
    }

    public static getPedidos(): Array<Pedido> {
        var pedidoList: Array<Pedido> = [];
        var usuarios = this.getUsuarios();
        var estadosPedido = this.getEstadosPedido();
        pedidoList.push({
            id: 1,
            usuario: usuarios[1],
            total: 10,
            estado: estadosPedido[0]
        });
        pedidoList.push({
            id: 2,
            usuario: usuarios[2],
            total: 10,
            estado: estadosPedido[0]
        });
        pedidoList.push({
            id: 3,
            usuario: usuarios[3],
            total: 10,
            estado: estadosPedido[2]
        });
        return pedidoList;
    }

    public static getDetallePedidos(): Array<DetallePedido> {
        var detallePedidoList: Array<DetallePedido> = [];
        var productos = this.getProductos();
        var pedidos = this.getPedidos();
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
        return detallePedidoList;
    }

    public static getMovimientosInventario(): Array<MovimientoInventario> {
        var movimientoInventarioList: Array<MovimientoInventario> = [];
        var productos = this.getProductos();
        var lotes = this.getLotesInventario();
        var tiposMovimiento = this.getTiposMovimiento();
        movimientoInventarioList.push({
            id: 1,
            producto: productos[0],
            lote: lotes[0],
            tipo_movimiento: tiposMovimiento[0],
            cantidad: 100,
            fecha: new Date()
        });
        movimientoInventarioList.push({
            id: 2,
            producto: productos[0],
            lote: lotes[1],
            tipo_movimiento: tiposMovimiento[0],
            cantidad: 50,
            fecha: new Date()
        });
        movimientoInventarioList.push({
            id: 3,
            producto: productos[1],
            lote: lotes[2],
            tipo_movimiento: tiposMovimiento[0],
            cantidad: 75,
            fecha: new Date()
        });
        movimientoInventarioList.push({
            id: 4,
            producto: productos[2],
            lote: lotes[3],
            tipo_movimiento: tiposMovimiento[0],
            cantidad: 150,
            fecha: new Date()
        });
        return movimientoInventarioList;
    }


    public static getPasarelas(): Array<Pasarela> {
        var pasarelaList: Array<Pasarela> = [];
        pasarelaList.push({
            id: 1,
            nombre: 'Wompi',
            descripcion: 'wompi el salvador',
            comision: 1.50
        });
        pasarelaList.push({
            id: 2,
            nombre: 'Serfinsa',
            descripcion: 'serfinsa el salvador',
            comision: 1.50
        });
        pasarelaList.push({
            id: 3,
            nombre: 'PayPal',
            descripcion: 'PayPal international',
            comision: 1.50
        });

        return pasarelaList;
    }



}
