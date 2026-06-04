import { EstadoProducto } from "./estado-producto";

export interface Producto {
    id: number;
    sku:string;
    nombre: string;
    descripcion: string;
    precio_venta:number;
    estado: EstadoProducto;
}
