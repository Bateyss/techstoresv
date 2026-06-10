import { Component, DOCUMENT, Inject, inject, OnInit, signal } from '@angular/core';
import { UntypedFormGroup } from '@angular/forms';
import { MAT_DIALOG_DATA, MatDialog, MatDialogRef } from '@angular/material/dialog';
import { MatDividerModule } from '@angular/material/divider';
import { MatSelectChange } from '@angular/material/select';
import { MatSnackBar } from '@angular/material/snack-bar';
import { MatStepperModule } from '@angular/material/stepper';
import { NavigationExtras, Router } from '@angular/router';
import { MaterialFormDialogModule, MaterialTableModelModule } from '../../material/material.module';
import { DetallePedido } from '../../models/detalle-pedido';
import { FieldConfig } from '../../models/field-config';
import { Pasarela } from '../../models/pasarela';
import { Pedido } from '../../models/pedido';
import { Producto } from '../../models/producto';
import { Usuario } from '../../models/usuario';
import { CarritoService } from '../../services/carrito-service';
import { DataService } from '../../services/data.service';
import { FormService } from '../../services/form-service';
import { UsuarioService } from '../../services/usuario-service';
import { DataApp } from '../../util/data-app';
import { FormErrorStateMatcher } from '../../util/form-error-state-matcher';
import { Utils } from '../../util/utils';

@Component({
  selector: 'app-ventas',
  imports: [MaterialTableModelModule],
  templateUrl: './ventas.html',
  styleUrl: './ventas.css',
})
export class Ventas implements OnInit {

  private _snackBar = inject(MatSnackBar);
  public innerWidths = '0';
  private document = inject(DOCUMENT);
  private dialog = inject(MatDialog);

  private usuarioService = inject(UsuarioService);
  private carritoService = inject(CarritoService);

  private usuario = signal<Usuario>(DataApp.usuarioVacio());
  private pedido = signal<Pedido>(DataApp.pedidoVacio());
  public detallePedidoList = signal<Array<DetallePedido>>([]);

  constructor(private _router: Router,) { }

  ngOnInit(): void {
    this.innerWidths = (this.document.body.clientWidth * 0.9) + 'px';
    this.usuario.update(valor => this.usuarioService.getUsuarioLoggeado());
    this.cargarListas();
  }

  cargarListas() {
    this.pedido.update(valor => this.carritoService.getUltimoPedidoUsuario(this.usuario()));
    this.detallePedidoList.update(valores => [...this.carritoService.getDetallesPedido(this.pedido())]);
  }

  aumentarDetallePedido(detalle: DetallePedido) {
    this.detallePedidoList.update(valores => [...this.carritoService.aumentarDetallePedido(detalle)]);
  }

  disminuirDetallePedido(detalle: DetallePedido) {
    this.detallePedidoList.update(valores => [...this.carritoService.disminuirDetallePedido(detalle)]);
  }

  pagoCarrito() {
    const dialogConfig = Utils.getMatDialogConf()
    dialogConfig.data = {
      detallePedidoList: this.detallePedidoList()
    };
    dialogConfig.width = '90vh';
    dialogConfig.height = '90vh';
    dialogConfig.maxWidth = '100vh';

    const dialogRef = this.dialog.open(PagarVentaDialog, dialogConfig);
    dialogRef.afterClosed().subscribe(
      result => {
        this.cargarListas();
      });
  }

  agregarDetallePedido() {

    //validar usuario
    if (this.usuario() && this.usuario().id > 0) {
      const dialogConfig = Utils.getMatDialogConf()
      dialogConfig.data = {
        pedido: this.pedido()
      };

      const dialogRef = this.dialog.open(AgregarDetallePedidoVentaDialog, dialogConfig);
      dialogRef.afterClosed().subscribe(
        result => {
          this.cargarListas();
        });
    } else {
      Utils.openSnackBar('Usuario no registrado', 'aceptar', this._snackBar);
      let navigationExtras: NavigationExtras = {
        queryParams: {
          "logged": 'true'
        }
      };
      this._router.navigate(['/menu/login'], navigationExtras);
    }
  }
}


@Component({
  selector: 'dialog-crear',
  imports: [MaterialFormDialogModule, MatStepperModule],
  templateUrl: './pago.venta.html',
  styleUrl: './ventas.css'
})
export class PagarVentaDialog implements OnInit {

  public innerWidths = '0';
  private document = inject(DOCUMENT);

  private _snackBar = inject(MatSnackBar);
  readonly dialogRef = inject(MatDialogRef<PagarVentaDialog>);
  public formConfigsCliente = signal<FieldConfig[]>([]);
  public formConfigsTipoPago = signal<FieldConfig[]>([]);
  public formConfigsTarjeta = signal<FieldConfig[]>([]);
  public matcher = new FormErrorStateMatcher();

  private formService = inject(FormService);
  private dataService = inject(DataService);
  private carritoService = inject(CarritoService);
  private usuarioService = inject(UsuarioService);

  public datosClienteForm!: UntypedFormGroup;
  public datosTarjetaForm!: UntypedFormGroup;
  public tipoPagoForm!: UntypedFormGroup;

  public detallePedidoList = signal<Array<DetallePedido>>([]);

  public pasarelaList = signal<Array<Pasarela>>([]);

  public totalPedido = signal<number>(0.00);
  public comisionPasarela = signal<number>(0.00);
  public totalPago = signal<number>(0.00);

  public esPagoTarjeta = signal(false);


  constructor(@Inject(MAT_DIALOG_DATA) public data: {
    detallePedidoList: Array<DetallePedido>
  }) {
    this.innerWidths = (this.document.body.clientWidth * 0.9) + 'px';
    this.cargarDatos(data.detallePedidoList);
  }

  ngOnInit() {
    this.cargarListas();

    var newConfigCliente = this.formService.newVentaLocalClienteFormControls();
    var newConfigTipoPago = this.formService.newVentaLocalTipoPagoFormControls();
    var newConfigTarjeta = this.formService.newVentaLocalFormControls(this.pasarelaList());

    this.datosClienteForm = this.formService.getFormGroup(newConfigCliente);
    this.tipoPagoForm = this.formService.getFormGroup(newConfigTipoPago);
    this.datosTarjetaForm = this.formService.getFormGroup(newConfigTarjeta);

    this.formConfigsCliente.update(actual => [...newConfigCliente]);
    this.formConfigsTipoPago.update(actual => [...newConfigTipoPago]);
    this.formConfigsTarjeta.update(actual => [...newConfigTarjeta]);

    this.cargarDatosForm();
  }


  getFormControlCliente(control: string) {
    return this.datosClienteForm.get(control);
  }
  getFormControlTipoPago(control: string) {
    return this.tipoPagoForm.get(control);
  }
  getFormControlTarjeta(control: string) {
    return this.datosTarjetaForm.get(control);
  }

  compareIds(id1: any, id2: any): boolean {
    return id1.id == id2.id;
  }

  cargarDatos(datos: any) {
    //this.detallePedidoList.update(valores => [...datos]);

    var usuarioLoggeado = this.usuarioService.getUsuarioLoggeado()
    var ultimoPedidoUsuario = this.carritoService.getUltimoPedidoUsuario(usuarioLoggeado)
    this.detallePedidoList.update(valores => [...this.carritoService.getDetallesPedido(ultimoPedidoUsuario)]);
  }

  cargarDatosForm() {
    if (this.detallePedidoList() && this.detallePedidoList().length > 0) {
      let sumPedido = 0.00;
      for (const detalle of this.detallePedidoList()) {
        sumPedido += (detalle.precio_unitario_venta * detalle.cantidad);
      }
      this.totalPedido.update(valor => sumPedido);
      this.totalPago.update(valor => sumPedido);
    }
  }

  cargarListas() {
    this.pasarelaList.update(valores => [...this.dataService.getPasarelas()]);
  }

  onSelectionTipoPagoChange(event: MatSelectChange) {
    const selectedValue = event.value;
    console.log('selected val');
    console.log(selectedValue);
    
    if (selectedValue == 2) {
      this.esPagoTarjeta.update(valor => true);
    }
    if (selectedValue == 1) {
      this.esPagoTarjeta.update(valor => false);
    }
  }

  pagarPedido() {
    let pagar = false;
    if (this.validarDatosCliente()) {
      if (this.validarDatosFormaPago()) {
        if (this.esPagoTarjeta()) {
          if (this.validarDatosTarjeta()) {
            pagar = true;
          } else {
            Utils.openSnackBar('Datos de tarjeta incorrectos', 'aceptar', this._snackBar);
          }
        } else {
          pagar = true;
        }

        if (pagar && this.totalPago() > 0 && this.totalPedido() > 0) {
          var pedido = this.detallePedidoList()[0].pedido;
          this.carritoService.pedidoPagado(pedido);
          Utils.openSnackBar('Pago exitoso', 'aceptar', this._snackBar);
          this.dialogRef.close();
        } else {
          Utils.openSnackBar('Datos de cliente incorrectos', 'aceptar', this._snackBar);
        }
      } else {
        Utils.openSnackBar('Datos de forma de pago incorrectos', 'aceptar', this._snackBar);
      }
    }
    else
      Utils.openSnackBar('Datos de cliente incorrectos', 'ok', this._snackBar);
  }

  validarDatosCliente(): boolean {
    var valido = this.formService.validateFormControls(this.datosClienteForm);
    return valido;
  }

  validarDatosFormaPago(): boolean {
    var valido = this.formService.validateFormControls(this.tipoPagoForm);
    return valido;
  }

  validarDatosTarjeta(): boolean {
    var valido = this.formService.validateFormControls(this.datosTarjetaForm);
    return valido;
  }

}


@Component({
  selector: 'dialog-agregar',
  imports: [MaterialFormDialogModule, MatDividerModule],
  templateUrl: './agregar.venta.html',
  styleUrl: './ventas.css'
})
export class AgregarDetallePedidoVentaDialog implements OnInit {

  public innerWidths = '0';
  private document = inject(DOCUMENT);

  private _snackBar = inject(MatSnackBar);
  readonly dialogRef = inject(MatDialogRef<AgregarDetallePedidoVentaDialog>);
  public formConfigs = signal<FieldConfig[]>([]);
  public matcher = new FormErrorStateMatcher();

  private formService = inject(FormService);
  private carritoService = inject(CarritoService);
  private dataService = inject(DataService);
  private usuarioService = inject(UsuarioService);

  public agregarCarritoForm!: UntypedFormGroup;

  public productoList = signal<Array<Producto>>([]);
  public pedido = DataApp.pedidoVacio();

  constructor(@Inject(MAT_DIALOG_DATA) public data: {
    pedido: Pedido
  }) {
    this.innerWidths = (this.document.body.clientWidth * 0.9) + 'px';
  }

  ngOnInit() {
    this.cargarDatos();
    this.cargarListas();

    var newConfig = this.formService.newAgregarDetallePedidoControls(this.productoList());
    this.agregarCarritoForm = this.formService.getFormGroup(newConfig);

    this.formConfigs.update(actual => [...newConfig]);
  }


  getFormControl(control: string) {
    return this.agregarCarritoForm.get(control);
  }

  compareIds(id1: any, id2: any): boolean {
    return id1.id == id2.id;
  }

  cargarDatos() {
    var usuarioLoggeado = this.usuarioService.getUsuarioLoggeado()
    this.pedido = this.carritoService.getUltimoPedidoUsuario(usuarioLoggeado)
  }

  cargarListas() {
    this.productoList.update(valores => [...this.carritoService.getProductosStocLocal()]);
  }

  agregarDetallePedido() {
    if (this.validarDatos()) {
      let datosForm = this.agregarCarritoForm.value;

      let detaLLePedido = DataApp.detallePedidoVacio();
      detaLLePedido.pedido = this.pedido;
      detaLLePedido.cantidad = datosForm.cantidad;
      detaLLePedido.producto = datosForm.producto;
      detaLLePedido.precio_unitario_venta = detaLLePedido.producto.precio_venta;
      this.dataService.pushDetallePedido(detaLLePedido);
      Utils.openSnackBar('Producto Agregado', 'aceptar', this._snackBar);
      this.dialogRef.close();
    }
    else
      Utils.openSnackBar('Datos incorrectos', 'ok', this._snackBar);
  }

  validarDatos(): boolean {
    var valido = this.formService.validateFormControls(this.agregarCarritoForm);
    var cantidadNum = parseFloat(this.getFormControl('cantidad')?.value);
    if (!cantidadNum || isNaN(cantidadNum)) {
      valido = false
      this.getFormControl('cantidad')?.setErrors({ key: "1" });
    };
    return valido;
  }

}
