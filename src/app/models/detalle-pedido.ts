import { Pedido } from "./pedido";
import { Producto } from "./producto";

export interface DetallePedido {
    id: number;
    pedido: Pedido;
    producto: Producto;
    cantidad: number;
    precio_unitario_venta: number;
}
