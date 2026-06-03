import { ChangeDetectionStrategy, Component, Inject, inject, OnInit, signal } from '@angular/core';
import { UntypedFormGroup } from '@angular/forms';
import { MAT_DIALOG_DATA, MatDialog, MatDialogConfig, MatDialogRef } from '@angular/material/dialog';
import { MatSnackBar } from '@angular/material/snack-bar';
import { MaterialModule } from '../../material/material.module';
import { EstadoProducto } from '../../models/estado-producto';
import { FieldConfig } from '../../models/field-config';
import { Producto } from '../../models/producto';
import { DataService } from '../../services/data.service';
import { FormService } from '../../services/form-service';
import { Utils } from '../../util/utils';
import { FormErrorStateMatcher } from '../../util/form-error-state-matcher';

@Component({
  selector: 'app-productos',
  imports: [MaterialModule],
  templateUrl: './productos.html',
  styleUrl: './productos.css',
})
export class Productos implements OnInit {

  public innerWidths = '0';

  private dialog = inject(MatDialog);
  private dataService = inject(DataService);

  public productoSeleccionado: Producto = DataService.productoVacio();

  public productosList = signal<Array<Producto>>([]);

  constructor() {
    this.innerWidths = (window.innerWidth * 0.9) + 'px';
  }
  ngOnInit(): void {
    this.cargarListas();
  }

  cargarListas() {
    this.productosList.update(() => [...this.dataService.getProductos()]);
  }

  seleccionarProducto(row: any) {
    this.productoSeleccionado = row;
  }

  crearProducto() {
    const dialogConfig = Utils.getMatDialogConf()
    dialogConfig.data = {
      productoSeleccionado: this.productoSeleccionado
    };

    const dialogRef = this.dialog.open(CrearProductoDialog, dialogConfig);
    dialogRef.afterClosed().subscribe(
      result => {
        this.cargarListas();
        this.productoSeleccionado = DataService.productoVacio();
      });
  }

  editarProducto(producto: Producto) {
    this.productoSeleccionado = producto;
    this.crearProducto();
  }

}

@Component({
  selector: 'dialog-crear',
  imports: [MaterialModule],
  templateUrl: './crear.producto.component.html',
  styleUrl: './productos.css'
})
export class CrearProductoDialog implements OnInit {

  public innerWidths = '0';

  private _snackBar = inject(MatSnackBar);
  private dataService = inject(DataService);
  readonly dialogRef = inject(MatDialogRef<CrearProductoDialog>);
  public formConfigs = signal<FieldConfig[]>([]);
  public matcher = new FormErrorStateMatcher();

  private formService = inject(FormService);
  public productoForm!: UntypedFormGroup;

  public productoSeleccionado: Producto = DataService.productoVacio();

  public estadoProductoList: Array<EstadoProducto> = [];

  constructor(@Inject(MAT_DIALOG_DATA) public data: {
    productoSeleccionado: Producto
  }) {
    this.innerWidths = (window.innerWidth * 0.9) + 'px';
    this.seleccionarProducto(data.productoSeleccionado);
  }

  ngOnInit() {
    this.cargarListas();

    var newConfig = this.formService.newProductoControls(this.estadoProductoList);
    this.productoForm = this.formService.getFormGroup(newConfig);

    this.formConfigs.update(actual => [...newConfig]);
    this.seleccionarProductoForm();
  }

  getFormControl(control: string) {
    return this.productoForm.get(control);
  }

  compareIds(id1: any, id2: any): boolean {
    return id1.id == id2.id;
  }

  seleccionarProducto(row: any) {
    this.productoSeleccionado = row;
  }

  seleccionarProductoForm() {
    this.getFormControl('id')?.setValue(this.productoSeleccionado.id);
    this.getFormControl('sku')?.setValue(this.productoSeleccionado.sku);
    this.getFormControl('nombre')?.setValue(this.productoSeleccionado.nombre);
    this.getFormControl('descripcion')?.setValue(this.productoSeleccionado.descripcion);
    this.getFormControl('precio_venta')?.setValue(this.productoSeleccionado.precio_venta);
    this.getFormControl('estado')?.setValue(this.productoSeleccionado.estado);
  }

  cargarListas() {
    this.estadoProductoList = this.dataService.getEstadosProducto();
  }

  guardarProducto() {
    if (this.validarDatos()) {

      if (this.productoSeleccionado.id > 0) {
        var idRef = this.productoSeleccionado.id;
        this.productoSeleccionado = this.productoForm.value;
        this.productoSeleccionado.id = idRef;
        this.dataService.editarProducto(this.productoSeleccionado);
        Utils.openSnackBar('CAMBIOS GUARDADOS EXITOSAMENTE', 'aceptar', this._snackBar);
      } else {
        this.dataService.pushProducto(this.productoSeleccionado);
        Utils.openSnackBar('NUEVA PLANILLA CREADA EXITOSAMENTE', 'aceptar', this._snackBar);
      }

      this.dialogRef.close();
    }
    else
      Utils.openSnackBar('Datos incorrectos', 'ok', this._snackBar);
  }

  validarDatos(): boolean {
    var valido = this.formService.validateFormControls(this.productoForm);
    var precioNum = parseFloat(this.getFormControl('precio_venta')?.value);
    if (!precioNum || isNaN(precioNum)) {
      valido = false
      this.getFormControl('precio_venta')?.setErrors({ key: "1" });
    };
    return valido;
  }

}
