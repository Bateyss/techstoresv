import { Routes } from '@angular/router';
import { Inicio } from './components/inicio/inicio';
import { Productos } from './components/productos/productos';
import { Login } from './components/login/login';
import { Carrito } from './components/carrito/carrito';

export const routes: Routes = [
    { path: '', redirectTo: '/menu', pathMatch: 'full' },
    {
        path: 'menu', component: Inicio, children: [
            { path: '', redirectTo: '/menu/login', pathMatch: 'full' },
            { path: 'login', component: Login, },
            { path: 'productos', component: Productos, },
            { path: 'carrito', component: Carrito, },
        ]
    }
];
