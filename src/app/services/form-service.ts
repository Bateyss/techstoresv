import { inject, Injectable } from '@angular/core';
import { UntypedFormBuilder, UntypedFormGroup, Validators } from '@angular/forms';
import { DataService } from './data.service';
import { FieldConfig } from '../models/field-config';
import { EstadoProducto } from '../models/estado-producto';
import { SelectOption } from '../models/select-option';
import { Producto } from '../models/producto';

@Injectable({
  providedIn: 'root',
})
export class FormService {

  private formBuilder = inject(UntypedFormBuilder);

  validateFormControls(formm: UntypedFormGroup): boolean {
    var valido = formm.valid;
    var controls = formm.getRawValue();
    for (let index = 0; index < controls.length; index++) {
      if (controls[index]?.enabled && !controls[index]?.valid) valido = false;
    }
    return valido;
  }

  getFormGroup(fields: FieldConfig[]): UntypedFormGroup {
    var formGroupConfigs: any = {};
    if (fields && fields.length > 0) {
      fields.forEach(field => formGroupConfigs[field.name] = [field.value || '', field.validators || []]);
    }
    
    return this.formBuilder.group(formGroupConfigs);
  }

  newProductoControls(estadosProducto: Array<EstadoProducto>): FieldConfig[] {
    var selectionEstados: Array<SelectOption> = [];
    if (estadosProducto && estadosProducto.length > 0) {
      estadosProducto.forEach(
        modelo => selectionEstados.push({ id: modelo.id, name: modelo.descripcion, value: modelo })
      );
    }
    return [
      { id: 1, name: 'sku', label: 'Codigo', type: 'text', controlType: 'input', validators: [Validators.required, Validators.minLength(3)] },
      { id: 2, name: 'nombre', label: 'Nombre Producto', type: 'text', controlType: 'input', validators: [Validators.required, Validators.minLength(5)] },
      { id: 3, name: 'descripcion', label: 'Descripcion', type: 'text', controlType: 'input', validators: [Validators.required, Validators.minLength(5)] },
      { id: 4, name: 'precio_venta', value: 0, label: 'Precio de Venta', type: 'text', controlType: 'input', validators: [Validators.required, Validators.min(0.01)] },
      { id: 5, name: 'estado', label: 'Estado', type: 'text', controlType: 'select', validators: [Validators.required], options: selectionEstados }
    ];
  }

  newCompraFormControls(productoList: Array<Producto>): FieldConfig[] {
    var selectionProductos: Array<SelectOption> = [];
    if (productoList && productoList.length > 0) {
      productoList.forEach(
        modelo => selectionProductos.push({ id: modelo.id, name: modelo.nombre, value: modelo })
      );
    }
    return [
      { id: 1, name: 'producto', label: 'Producto', type: 'text', controlType: 'select', validators: [Validators.required], options: selectionProductos },
      { id: 2, name: 'cantidad', value: 0, label: 'Cantidad', type: 'number', controlType: 'input', validators: [Validators.required, Validators.min(0.01)] },
      { id: 3, name: 'pagoTotal', value: 0, label: 'Pago Total', type: 'number', controlType: 'input', validators: [Validators.required, Validators.min(0.01)] },
    ];
  }


  newVentaForm(): UntypedFormGroup {
    return this.formBuilder.group({
      sku: ['', [Validators.required, Validators.minLength(5)]],
      nombre: ['', [Validators.required, Validators.minLength(3)]],
      descripcion: ['', [Validators.required, Validators.minLength(3)]],
      precio_venta: [0, [Validators.required, Validators.min(0.01)]],
      estado: [DataService.estadoProductoVacio(), [Validators.required]]
    });
  }

  newMermaForm(): UntypedFormGroup {
    return this.formBuilder.group({
      sku: ['', [Validators.required, Validators.minLength(5)]],
      nombre: ['', [Validators.required, Validators.minLength(3)]],
      descripcion: ['', [Validators.required, Validators.minLength(3)]],
      precio_venta: [0, [Validators.required, Validators.min(0.01)]],
      estado: [DataService.estadoProductoVacio(), [Validators.required]]
    });
  }

  newDevolucionCompraForm(): UntypedFormGroup {
    return this.formBuilder.group({
      sku: ['', [Validators.required, Validators.minLength(5)]],
      nombre: ['', [Validators.required, Validators.minLength(3)]],
      descripcion: ['', [Validators.required, Validators.minLength(3)]],
      precio_venta: [0, [Validators.required, Validators.min(0.01)]],
      estado: [DataService.estadoProductoVacio(), [Validators.required]]
    });
  }

  newDevolucionVentaForm(): UntypedFormGroup {
    return this.formBuilder.group({
      sku: ['', [Validators.required, Validators.minLength(5)]],
      nombre: ['', [Validators.required, Validators.minLength(3)]],
      descripcion: ['', [Validators.required, Validators.minLength(3)]],
      precio_venta: [0, [Validators.required, Validators.min(0.01)]],
      estado: [DataService.estadoProductoVacio(), [Validators.required]]
    });
  }

}
