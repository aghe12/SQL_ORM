import { User } from "./entities/user.entity.js";
import { Employee } from "./entities/employee.entity.js";

const newUser = new User({
    id: 1,
    name: 'John Doe',
    address: '123 Main St',
    dob: new Date('1990-01-01'),
    email: 'john.doe@example.com',
    createdAt: new Date(),
    createdBy: 1,
    updatedAt: new Date(),
    updatedBy: 1
});
await newUser.save();

const foundUser = await User.findById(1);
console.log(foundUser);

const newEmployee = new Employee({
    id: 1,
    name: 'Jane Smith',
    position: 'Software Engineer',
    department: 'Engineering',
    salary: 90000,
    createdAt: new Date(),
    createdBy: 1,
    updatedAt: new Date(),
    updatedBy: 1
});
await newEmployee.save();

const foundEmployee = await Employee.findById(1);
console