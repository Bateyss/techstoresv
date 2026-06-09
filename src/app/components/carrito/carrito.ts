import { Component, DOCUMENT, Inject, inject, OnInit, signal } from '@angular/core';
import { UntypedFormGroup } from '@angular/forms';
import { MAT_DIALOG_DATA, MatDialog, MatDialogRef } from '@angular/material/dialog';
import { MatDividerModule } from "@angular/material/divider";
import { MatSelectChange } from '@angular/material/select';
import { MatSnackBar } from '@angular/material/snack-bar';
import { MaterialFormDialogModule, MaterialTableModelModule } from '../../material/material.module';
import { DetallePedido } from '../../models/detalle-pedido';
import { FieldConfig } from '../../models/field-config';
import { Pasarela } from '../../models/pasarela';
import { Pedido } from '../../models/pedido';
import { Usuario } from '../../models/usuario';
import { CarritoService } from '../../services/carrito-service';
import { DataService } from '../../services/data.service';
import { FormService } from '../../services/form-service';
import { UsuarioService } from '../../services/usuario-service';
import { DataApp } from '../../util/data-app';
import { FormErrorStateMatcher } from '../../util/form-error-state-matcher';
import { Utils } from '../../util/utils';

@Component({
  selector: 'app-carrito',
  imports: [MaterialTableModelModule],
  templateUrl: './carrito.html',
  styleUrl: './carrito.css',
})
export class Carrito implements OnInit {

  public innerWidths = '0';
  private document = inject(DOCUMENT);
  private dialog = inject(MatDialog);

  private usuarioService = inject(UsuarioService);
  private carritoService = inject(CarritoService);

  private usuario = signal<Usuario>(DataApp.usuarioVacio());
  private pedido = signal<Pedido>(DataApp.pedidoVacio());
  public detallePedidoList = signal<Array<DetallePedido>>([]);

  constructor() { }

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

    const dialogRef = this.dialog.open(PagarPedidoDialog, dialogConfig);
    dialogRef.afterClosed().subscribe(
      result => {
        this.cargarListas();
      });
  }
}

@Component({
  selector: 'dialog-crear',
  imports: [MaterialFormDialogModule, MatDividerModule],
  templateUrl: './pago.carrito.html',
  styleUrl: './carrito.css'
})
export class PagarPedidoDialog implements OnInit {

  public innerWidths = '0';
  private document = inject(DOCUMENT);

  private _snackBar = inject(MatSnackBar);
  readonly dialogRef = inject(MatDialogRef<PagarPedidoDialog>);
  public formConfigs = signal<FieldConfig[]>([]);
  public matcher = new FormErrorStateMatcher();

  private formService = inject(FormService);
  private dataService = inject(DataService);
  private carritoService = inject(CarritoService);
  private usuarioService = inject(UsuarioService);

  public pagoCarritoForm!: UntypedFormGroup;

  public detallePedidoList = signal<Array<DetallePedido>>([]);

  public pasarelaList = signal<Array<Pasarela>>([]);

  public totalPedido = signal<number>(0.00);
  public comisionPasarela = signal<number>(0.00);
  public totalPago = signal<number>(0.00);


  constructor(@Inject(MAT_DIALOG_DATA) public data: {
    detallePedidoList: Array<DetallePedido>
  }) {
    this.innerWidths = (this.document.body.clientWidth * 0.9) + 'px';
    this.cargarDatos(data.detallePedidoList);
  }

  ngOnInit() {
    this.cargarListas();

    var newConfig = this.formService.newVentaLineaFormControls(this.pasarelaList());
    this.pagoCarritoForm = this.formService.getFormGroup(newConfig);

    this.formConfigs.update(actual => [...newConfig]);
    this.cargarDatosForm();
  }


  getFormControl(control: string) {
    return this.pagoCarritoForm.get(control);
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

  onSelectionChange(event: MatSelectChange) {
    const selectedValue = event.value;
    this.comisionPasarela.update(valor => selectedValue.comision);
    this.totalPago.update(valor => this.totalPedido() + selectedValue.comision);
  }

  pagarPedido() {
    if (this.validarDatos()) {

      if (this.totalPago() > 0 && this.totalPedido() > 0) {
        var pedido = this.detallePedidoList()[0].pedido;
        this.carritoService.pedidoPagado(pedido);

        Utils.openSnackBar('Pago exitoso', 'aceptar', this._snackBar);
        this.dialogRef.close();
      } else {
        Utils.openSnackBar('Datos Incorrectos', 'aceptar', this._snackBar);
      }
    }
    else
      Utils.openSnackBar('Datos incorrectos', 'ok', this._snackBar);
  }

  validarDatos(): boolean {
    var valido = this.formService.validateFormControls(this.pagoCarritoForm);
    return valido;
  }

}
