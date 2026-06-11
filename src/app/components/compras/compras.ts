import { Component, DOCUMENT, Inject, inject, OnInit, signal } from '@angular/core';
import { UntypedFormGroup } from '@angular/forms';
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

  public detallePedidoList = signal<Array<DetallePedido>>([]);

  constructor() { }

  ngOnInit(): void {
    this.innerWidths = (this.document.body.clientWidth * 0.9) + 'px';
    this.cargarListas();
  }

  cargarListas() {
    this.detallePedidoList.update(valores => [...this.compraService.getDetallePedidosCompra()]);
  }

  registrarCompra() {
    this.compraService.registrarCompra();
    this.cargarListas();
  }

  aumentarDetalleCompra(detalle: DetallePedido) {
    this.detallePedidoList.update(valores => [...this.compraService.aumentarDetalleCompra(detalle)]);
  }

  disminuirDetalleCompra(detalle: DetallePedido) {
    this.detallePedidoList.update(valores => [...this.compraService.disminuirDetalleCompra(detalle)]);
  }

  agregarDetalleCompra() {

    const dialogConfig = Utils.getMatDialogConf()
    dialogConfig.data = {
      pedido: DataApp.pedidoVacio()
    };

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
  public pedido = DataApp.pedidoVacio();

  constructor(@Inject(MAT_DIALOG_DATA) public data: {
    pedido: Pedido
  }) {
    this.innerWidths = (this.document.body.clientWidth * 0.9) + 'px';
  }

  ngOnInit() {
    this.cargarListas();

    var newConfig = this.formService.newAgregarDetallePedidoControls(this.productoList());
    this.agregarCompraForm = this.formService.getFormGroup(newConfig);

    this.formConfigs.update(actual => [...newConfig]);
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

      let detaLLePedido = DataApp.detallePedidoVacio();
      detaLLePedido.pedido = this.pedido;
      detaLLePedido.cantidad = datosForm.cantidad;
      detaLLePedido.producto = datosForm.producto;
      detaLLePedido.precio_unitario_venta = detaLLePedido.producto.precio_venta;
      this.compraService.pushDetalleCompra(detaLLePedido);
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
