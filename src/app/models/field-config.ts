import { SelectOption } from "./select-option";

export interface FieldConfig {
    id: number;
    name: string;
    label: string;
    value?: any;
    type: 'text' | 'email' | 'password' | 'number' ;
    controlType: 'input' | 'select' | 'textarea';
    options?: Array<SelectOption>;
    validators?: any[];
}
