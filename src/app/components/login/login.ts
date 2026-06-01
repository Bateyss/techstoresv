import { Component, DOCUMENT, inject, Inject } from '@angular/core';
import { UntypedFormBuilder, UntypedFormGroup, Validators } from '@angular/forms';
import { NavigationExtras, Router } from '@angular/router';
import { MaterialModule } from '../../material/material.module';
import { MatSnackBar } from '@angular/material/snack-bar';
import { Utils } from '../../util/utils';
import { DataService } from '../../services/data.service';
import { UsuarioService } from '../../services/usuario-service';

@Component({
  selector: 'app-login',
  imports: [MaterialModule],
  templateUrl: './login.html',
  styleUrl: './login.css',
})
export class Login {

  //forms
  public loginForm: UntypedFormGroup;

  public mensajeLogin = '';

  readonly _snackBar = inject(MatSnackBar);
  readonly usuarioService = inject(UsuarioService);

  constructor(@Inject(DOCUMENT) private document: Document,
    private _router: Router,
    private formBuilder: UntypedFormBuilder) {
    this.loginForm = this.formBuilder.group({
      username: ['', [Validators.required, Validators.minLength(3)]],
      password: ['', [Validators.required, Validators.minLength(3)]]
    });
  }

  login() {
    if (this.validarDatos()) {
      if (this.loginF('username')?.value === 'admin' && this.loginF('password')?.value === '1234') {
        Utils.openSnackBar('Login exitoso', 'ok', this._snackBar);
        this.document.defaultView?.localStorage?.setItem('usuario', this.loginF('username')?.value);
        this.document.defaultView?.localStorage?.setItem('logged', 'true');
        let navigationExtras: NavigationExtras = {
          queryParams: {
            "logged": 'true'
          }
        };
        this._router.navigate(['/menu/planilla'], navigationExtras);
      } else {
        Utils.openSnackBar('Credenciales incorrectas', 'ok', this._snackBar);
      }
    } else {
      Utils.openSnackBar('Completar credenciales', 'ok', this._snackBar);
    }
  }

  validarDatos() {
    var valido = this.loginForm.valid;
    if (!this.loginF('username')?.valid) valido = false;
    if (!this.loginF('password')?.valid) valido = false;
    return valido;
  }

  loginF(control: string) { return this.loginForm.get(control); }

}
