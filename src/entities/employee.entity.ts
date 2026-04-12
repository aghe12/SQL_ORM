import { BaseEntity, type IBaseEntity } from "./base.entity.js";
import { Table, TABLE_METADATA_KEY } from "./table.decorator.js";

export interface IEmployee extends IBaseEntity {

    name: string;
    position: string;
    department: string;
    salary: number;
}

@Table('employees')
export class Employee extends BaseEntity implements IEmployee {

    name: string;
    position: string;
    department: string;
    salary: number;

    constructor(employee: IEmployee) {
        super(employee);
        this.name = employee.name;
        this.position = employee.position;
        this.department = employee.department;
        this.salary = employee.salary;
    }

}