import { Producto } from "./producto";

export interface LoteInventario {
    id: number,
    producto: Producto,
    cantidad_inicial: number,
    cantidad_actual: number,
    costo_unitario: number,
    fecha_ingreso: Date
}
