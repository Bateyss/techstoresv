import { Producto } from "./producto";

export interface DetalleCompra {
    id: number;
    producto: Producto;
    cantidad: number;
    cantidad_web: number;
    cantidad_local: number;
    precio_unitario_venta: number;
}
