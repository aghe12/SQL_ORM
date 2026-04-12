import { BaseEntity, type IBaseEntity } from "./base.entity.js";
import { Table, TABLE_METADATA_KEY } from "./table.decorator.js";


export interface IUser extends IBaseEntity {
    name: string;
    address: string;
    dob: Date;
    email: string;
}

@Table('users')
export class User extends BaseEntity implements IUser {

    name: string;
    address: string;
    dob: Date;
    email: string;

    constructor(user: IUser) {
        super(user);
        this.name = user.name;
        this.address = user.address;
        this.dob = user.dob;
        this.email = user.email;
    }
    
}