import { Component, inject } from '@angular/core';
import { UntypedFormBuilder, UntypedFormGroup, Validators } from '@angular/forms';
import { MatSnackBar } from '@angular/material/snack-bar';
import { NavigationExtras, Router } from '@angular/router';
import { MaterialFormModule } from '../../material/material.module';
import { LocalStorageService } from '../../services/local-storage-service';
import { UsuarioService } from '../../services/usuario-service';
import { DataApp } from '../../util/data-app';
import { FormErrorStateMatcher } from '../../util/form-error-state-matcher';
import { Utils } from '../../util/utils';

@Component({
  selector: 'app-login',
  imports: [MaterialFormModule],
  templateUrl: './login.html',
  styleUrl: './login.css',
})
export class Login {

  //forms
  public loginForm: UntypedFormGroup;

  public mensajeLogin = '';

  private _snackBar = inject(MatSnackBar);
  private usuarioService = inject(UsuarioService);
  private localStorage = inject(LocalStorageService);

  public matcher = new FormErrorStateMatcher();

  constructor(
    private _router: Router,
    private formBuilder: UntypedFormBuilder) {
    this.loginForm = this.formBuilder.group({
      username: ['', [Validators.required, Validators.minLength(3)]],
      password: ['', [Validators.required, Validators.minLength(3)]]
    });
  }

  login() {
    if (this.validarDatos()) {
      var usuario = this.usuarioService.validarUsuario(this.loginF('username')?.value, this.loginF('password')?.value);
      if (usuario && usuario.id > 1) {
        Utils.openSnackBar('Login exitoso', 'ok', this._snackBar);
        this.localStorage.setItem(DataApp.LOGGED_USUARIO, usuario);
        this.localStorage.setItem(DataApp.LOGGED, 'true');
        let navigationExtras: NavigationExtras = {
          queryParams: {
            "logged": 'true'
          }
        };
        this._router.navigate(['/menu/productos'], navigationExtras);
      } else {
        Utils.openSnackBar('Credenciales incorrectas', 'ok', this._snackBar);
      }
    } else {
      Utils.openSnackBar('Completar credenciales', 'ok', this._snackBar);
    }
  }

  iniciarInvitado() {
    var usuario = this.usuarioService.getUsuarioAnonimo();
    this.localStorage.setItem(DataApp.LOGGED_USUARIO, usuario);
    this.localStorage.setItem(DataApp.LOGGED, 'true');
    let navigationExtras: NavigationExtras = {
      queryParams: {
        "logged": 'true'
      }
    };
    this._router.navigate(['/menu/productos'], navigationExtras);
  }

  validarDatos() {
    var valido = this.loginForm.valid;
    if (!this.loginF('username')?.valid) valido = false;
    if (!this.loginF('password')?.valid) valido = false;
    return valido;
  }


  loginF(control: string) { return this.loginForm.get(control); }

}
