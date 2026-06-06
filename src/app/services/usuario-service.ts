import { inject, Injectable } from '@angular/core';
import { Usuario } from '../models/usuario';
import { DataApp } from '../util/data-app';
import { Utils } from '../util/utils';
import { DataService } from './data.service';
import { LocalStorageService } from './local-storage-service';

@Injectable({
  providedIn: 'root',
})
export class UsuarioService {

  private dataService = inject(DataService);
  private localStorageService = inject(LocalStorageService);

  constructor() { }

  getUsuarioAnonimo(): Usuario {
    return {
      id: 1,
      usuario: 'anonim',
      password: '',
      nombres: 'anonimo',
      apellidos: '',
      fecha_creacion: new Date()
    }
  }

  getUsuarios(): Array<Usuario> {
    return this.dataService.getUsuarios();
  }

  pushUsuario(datos: Usuario) {
    var usuarioList: Array<Usuario> = this.getUsuarios();
    datos.id = usuarioList[usuarioList.length - 1].id + 1;
    usuarioList.push(datos);
    this.localStorageService.setItem(DataApp.USUARIOS_ID, usuarioList);
  }

  editarUsuario(datos: Usuario) {
    var usuarioList: Array<Usuario> = this.getUsuarios();
    var usuarioListNueva: Array<Usuario> = [];

    usuarioList.forEach(arrData => {
      var newData: Usuario = arrData;
      if (newData.id == datos.id) {
        newData = datos;
        if (!Utils.isSha256(newData.password)) Utils.generateSHA256(newData.password).then(b => newData.password = b);
      }
      usuarioListNueva.push(newData);
    });

    this.localStorageService.setItem(DataApp.USUARIOS_ID, usuarioListNueva);
  }

  validarUsuario(user: string, pass: string): Usuario | null {
    var passEncrypted = '';
    Utils.generateSHA256(pass).then(b => passEncrypted = b);
    var usuarioList: Array<Usuario> = this.getUsuarios();

    var usuarioSesion = DataApp.usuarioVacio();

    if (usuarioList && usuarioList.length) {
      usuarioList
        .filter(usuario => usuario.usuario == user)
        .filter(usuario => usuario.password == passEncrypted)
        .forEach(usuario => usuarioSesion = usuario);
    }

    if (usuarioSesion && usuarioSesion.id > 1) {
      return usuarioSesion;
    } else {
      return null;
    }
  }

  getUsuarioLoggeado(): Usuario{
    var usuario = DataApp.usuarioVacio();
    var usuarioSesion = this.localStorageService.getItem<Usuario>('usuario');
    if (usuarioSesion) {
      usuario = usuarioSesion;
    }
    return usuario;
  }

}
