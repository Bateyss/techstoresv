import { Component, DOCUMENT, Inject, inject, OnInit, signal } from '@angular/core';
import { UntypedFormGroup, Validators } from '@angular/forms';
import { MAT_DIALOG_DATA, MatDialog, MatDialogRef } from '@angular/material/dialog';
import { MatDividerModule } from '@angular/material/divider';
import { MatSnackBar } from '@angular/material/snack-bar';
import { MaterialFormDialogModule, MaterialTableModelModule } from '../../material/material.module';
import { DetallePedido } from '../../models/detalle-pedido';
import { FieldConfig } from '../../models/field-config';
import { Pedido } from '../../models/pedido';
import { Producto } from '../../models/producto';
import { CompraService } from '../../services/compra-service';
import { DataService } from '../../services/data.service';
import { FormService } from '../../services/form-service';
import { DataApp } from '../../util/data-app';
import { FormErrorStateMatcher } from '../../util/form-error-state-matcher';
import { Utils } from '../../util/utils';
import { DetalleCompra } from '../../models/detalle-compra';

@Component({
  selector: 'app-compras',
  imports: [MaterialTableModelModule],
  templateUrl: './compras.html',
  styleUrl: './compras.css',
})
export class Compras implements OnInit {
  public innerWidths = '0';
  private document = inject(DOCUMENT);
  private dialog = inject(MatDialog);

  private compraService = inject(CompraService);

  public detalleCompraList = signal<Array<DetalleCompra>>([]);

  constructor() { }

  ngOnInit(): void {
    this.innerWidths = (this.document.body.clientWidth * 0.9) + 'px';
    this.cargarListas();
  }

  cargarListas() {
    this.detalleCompraList.update(valores => [...this.compraService.getdetalleCompras()]);
  }

  registrarCompra() {
    this.compraService.registrarCompra();
    this.cargarListas();
  }

  aumentarDetalleCompra(detalle: DetalleCompra) {
    this.detalleCompraList.update(valores => [...this.compraService.aumentarDetalleCompra(detalle)]);
  }

  disminuirDetalleCompra(detalle: DetalleCompra) {
    this.detalleCompraList.update(valores => [...this.compraService.disminuirDetalleCompra(detalle)]);
  }

  agregarDetalleCompra() {
    const dialogConfig = Utils.getMatDialogConf();
    const dialogRef = this.dialog.open(AgregarDetalleCompraDialog, dialogConfig);
    dialogRef.afterClosed().subscribe(
      result => {
        this.cargarListas();
      });
  }

}

@Component({
  selector: 'dialog-agregar',
  imports: [MaterialFormDialogModule, MatDividerModule],
  templateUrl: './agregar.compra.html',
  styleUrl: './compras.css'
})
export class AgregarDetalleCompraDialog implements OnInit {

  public innerWidths = '0';
  private document = inject(DOCUMENT);

  private _snackBar = inject(MatSnackBar);
  readonly dialogRef = inject(MatDialogRef<AgregarDetalleCompraDialog>);
  public formConfigs = signal<FieldConfig[]>([]);
  public matcher = new FormErrorStateMatcher();

  private formService = inject(FormService);
  private dataService = inject(DataService);
  private compraService = inject(CompraService);


  public agregarCompraForm!: UntypedFormGroup;

  public productoList = signal<Array<Producto>>([]);

  constructor() {
    this.innerWidths = (this.document.body.clientWidth * 0.9) + 'px';
  }

  ngOnInit() {
    this.cargarListas();

    var newConfig = this.formService.newAgregarDetalleCompraControls(this.productoList());
    this.agregarCompraForm = this.formService.getFormGroup(newConfig);

    this.formConfigs.update(actual => [...newConfig]);
  }

  inputChange(event: any) {
    let cantidad = this.getFormControl('cantidad');
    let cantidad_web = this.getFormControl('cantidad_web');
    let cantidad_local = this.getFormControl('cantidad_local');

    let cantidad_val = cantidad?.value | 0;
    let cantidad_web_val = cantidad_web?.value | 0;
    let cantidad_local_val = cantidad_local?.value | 0;

    if (cantidad_val > 0) {
      let cantidad_web_min = 0;
      let cantidad_web_max = cantidad_val;
      let cantidad_local_min = 0;
      let cantidad_local_max = cantidad_val;
      if (cantidad_web_val > 0) {
        cantidad_local_max = cantidad_val - cantidad_web_val;
      }
      if (cantidad_local_val > 0) {
        cantidad_web_max = cantidad_val - cantidad_local_val;
      }
      if ((cantidad_val - (cantidad_web_val + cantidad_local_val)) > 0) {
        cantidad_web_min = cantidad_val - (cantidad_web_val + cantidad_local_val);
        cantidad_local_min = cantidad_web_min;
      }
      cantidad_web?.setValidators([Validators.required, Validators.min(cantidad_web_min), Validators.max(cantidad_web_max)]);
      cantidad_local?.setValidators([Validators.required, Validators.min(cantidad_local_min), Validators.max(cantidad_local_max)]);
    }
  }

  getFormControl(control: string) {
    return this.agregarCompraForm.get(control);
  }

  compareIds(id1: any, id2: any): boolean {
    return id1.id == id2.id;
  }

  cargarListas() {
    this.productoList.update(valores => [...this.dataService.getProductos()]);
  }

  agregarDetalleCompra() {
    if (this.validarDatos()) {
      let datosForm = this.agregarCompraForm.value;

      if ((parseInt(datosForm.cantidad) - (parseInt(datosForm.cantidad_local) + parseInt(datosForm.cantidad_web))) > 0) {
        Utils.openSnackBar('Cantidades no cuadran', 'ok', this._snackBar);
        return;
      }

      let detaLLeCompra = DataApp.detalleCompraVacio();
      detaLLeCompra.cantidad = parseInt(datosForm.cantidad);
      detaLLeCompra.cantidad_local = parseInt(datosForm.cantidad_local);
      detaLLeCompra.cantidad_web = parseInt(datosForm.cantidad_web);
      detaLLeCompra.producto = datosForm.producto;
      detaLLeCompra.precio_unitario_venta = detaLLeCompra.producto.precio_venta;
      this.compraService.pushDetalleCompra(detaLLeCompra);
      Utils.openSnackBar('Producto Agregado', 'aceptar', this._snackBar);
      this.dialogRef.close();
    }
    else
      Utils.openSnackBar('Datos incorrectos', 'ok', this._snackBar);
  }

  validarDatos(): boolean {
    var valido = this.formService.validateFormControls(this.agregarCompraForm);
    var cantidadNum = parseFloat(this.getFormControl('cantidad')?.value);
    if (!cantidadNum || isNaN(cantidadNum)) {
      valido = false
      this.getFormControl('cantidad')?.setErrors({ key: "1" });
    };
    return valido;
  }

}
