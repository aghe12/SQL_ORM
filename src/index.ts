import { DB } from "./core/db.js";
import { MySqlDriver } from "./drivers/mysql.driver.js";
import { PostgreSqlDriver } from "./drivers/postgresql.driver.js";
import { Employee } from "./entities/employee.entity.js";
import { User } from "./entities/user.entity.js";



// DB.setDriver(new MySqlDriver("mysql://root:root123@localhost:3307/ORM"));

DB.setDriver(new PostgreSqlDriver('postgresql://user:user_password@localhost:5432/orm_db'));



async function bootstrap(): Promise<void> {
    try {
        await DB.driver.connect();
        console.log("Connected to database");

        // Generate unique emails with timestamp
        const timestamp = Date.now();
        
        // Insert first user
        const user1 = new User({
            name: 'Emma Thompson',
            address: '123 Main St',
            dob: new Date('1990-01-01'),
            email: `emma.thompson.${timestamp}@example.com`,
            createdAt: new Date(),
            createdBy: 1,
            updatedAt: new Date(),
            updatedBy: 1
        });
        await user1.save();
        console.log("User 1 created:", user1);

        // Insert second user
        const user2 = new User({
            name: 'David Martinez',
            address: '456 Oak Ave',
            dob: new Date('1985-05-15'),
            email: `david.martinez.${timestamp}@example.com`,
            createdAt: new Date(),
            createdBy: 1,
            updatedAt: new Date(),
            updatedBy: 1
        });
        await user2.save();
        console.log("User 2 created:", user2);

        // Insert third user
        const user3 = new User({
            name: 'Lisa Anderson',
            address: '789 Pine Rd',
            dob: new Date('1992-08-20'),
            email: `lisa.anderson.${timestamp}@example.com`,
            createdAt: new Date(),
            createdBy: 1,
            updatedAt: new Date(),
            updatedBy: 1
        });
        await user3.save();
        console.log("User 3 created:", user3);

        // Insert first employee
        const employee1 = new Employee({
            name: 'Jane Smith',
            position: 'Software Engineer',
            department: 'Engineering',
            salary: 90000,
            createdAt: new Date(),
            createdBy: 1,
            updatedAt: new Date(),
            updatedBy: 1
        });
        await employee1.save();
        console.log("Employee 1 created:", employee1);

        // Insert second employee
        const employee2 = new Employee({
            name: 'Mike Davis',
            position: 'Product Manager',
            department: 'Product',
            salary: 85000,
            createdAt: new Date(),
            createdBy: 1,
            updatedAt: new Date(),
            updatedBy: 1
        });
        await employee2.save();
        console.log("Employee 2 created:", employee2);

        // Insert third employee
        const employee3 = new Employee({
            name: 'Sarah Chen',
            position: 'UX Designer',
            department: 'Design',
            salary: 75000,
            createdAt: new Date(),
            createdBy: 1,
            updatedAt: new Date(),
            updatedBy: 1
        });
        await employee3.save();
        console.log("Employee 3 created:", employee3);

        // Find all users
        const allUsers = await User.findAll();
        console.log("All Users:", allUsers);

        // Find all employees
        const allEmployees = await Employee.findAll();
        console.log("All Employees:", allEmployees);
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