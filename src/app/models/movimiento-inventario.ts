import { LoteInventario } from "./lote-inventario";
import { Pedido } from "./pedido";
import { Producto } from "./producto";

export interface MovimientoInventario {
    id: number,
    producto: Producto,
    lote: LoteInventario,
    tipo_movimiento: number,
    cantidad: number,
    pedido: Pedido,
    fecha: Date
}
