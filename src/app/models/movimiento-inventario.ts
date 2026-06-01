import { LoteInventario } from "./lote-inventario";
import { Pedido } from "./pedido";
import { Producto } from "./producto";
import { TipoMovimiento } from "./tipo-movimiento";

export interface MovimientoInventario {
    id: number,
    producto: Producto,
    lote: LoteInventario,
    tipo_movimiento: TipoMovimiento,
    cantidad: number,
    pedido: Pedido | null,
    fecha: Date
}
