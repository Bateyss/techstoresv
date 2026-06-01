import { EstadoPedido } from "./estado-pedido"
import { Usuario } from "./usuario"

export interface Pedido {
    id: number,
    usuario: Usuario,
    total: number,
    estado: EstadoPedido
}
