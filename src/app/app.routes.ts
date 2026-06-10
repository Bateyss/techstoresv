import { Routes } from '@angular/router';
import { Carrito } from './components/carrito/carrito';
import { Inicio } from './components/inicio/inicio';
import { Login } from './components/login/login';
import { Productos } from './components/productos/productos';
import { Ventas } from './components/ventas/ventas';

export const routes: Routes = [
    { path: '', redirectTo: '/menu', pathMatch: 'full' },
    {
        path: 'menu', component: Inicio, children: [
            { path: '', redirectTo: '/menu/login', pathMatch: 'full' },
            { path: 'login', component: Login, },
            { path: 'productos', component: Productos, },
            { path: 'carrito', component: Carrito, },
            { path: 'ventas', component: Ventas, },
        ]
    }
];
