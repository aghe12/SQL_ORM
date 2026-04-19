import { DB } from "./core/db.js";
import { MySqlDriver } from "./drivers/mysql.driver.js";
import { PostgreSqlDriver } from "./drivers/postgresql.driver.js";
import { Employee } from "./entities/employee.entity.js";
import { User } from "./entities/user.entity.js";


// Choose your database driver:
// Option 1: MySQL
DB.setDriver(new MySqlDriver("mysql://root:root123@localhost:3307/ORM"));

// Option 2: PostgreSQL (uncomment to use)
// DB.setDriver(new PostgreSqlDriver('postgresql://postgres:password@localhost:5432/test'));



async function bootstrap(): Promise<void> {
    try {
        await DB.driver.connect();
        console.log("Connected to database");

        const newUser = new User({
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
        console.log(foundEmployee);
    } catch (err) {
        console.error("Application startup failed:", err);
    } finally {
        try {
            await DB.driver.disconnect();
            console.log("Disconnected from database");
        } catch (err) {
            console.error("Error disconnecting from database:", err);
        }
    }
}

void bootstrap();