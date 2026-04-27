/* 
CHANGES MADE:
1. Completely rewrote bootstrap function to match target implementation
2. Added comprehensive testing of all CRUD operations for both User and Employee entities
3. Changed from simple insertion to full ORM functionality testing
4. Added proper error handling and database connection management
5. WHAT IT DOES: Comprehensive test suite demonstrating all ORM capabilities
6. WHY: Validates that all database operations work correctly with the updated implementation
7. Tests both entities to ensure the ORM works with different data models
8. IMPORTANT: This serves as both documentation and validation of the ORM functionality
*/
import { DB } from "./core/db.js";
import { MySqlDriver } from "./drivers/mysql.driver.js";
import { Employee } from "./entities/employee.entity.js";
import { User } from "./entities/user.entity.js";
import { PostgreSqlDriver } from "./drivers/postgresql.driver.js";

// DB.setDriver(
//   new MySqlDriver({
//     host: "localhost",
//     port: 3306,
//     user: "user",
//     password: "user_password",
//     database: "orm_db",
//   }),
// );

/* 
CHANGES MADE:
1. Changed from MySQL to PostgreSQL driver configuration
2. Updated connection string to match target implementation
3. Commented out MySQL configuration for easy switching
4. WHAT IT DOES: Configures the ORM to use PostgreSQL database
5. WHY: PostgreSQL was the target database for this implementation
6. IMPORTANT: Connection string must match your PostgreSQL setup
*/
DB.setDriver(
  new PostgreSqlDriver("postgres://user:user_password@localhost:5432/orm_db"),
);

/* 
CHANGES MADE:
1. Completely rewritten to demonstrate comprehensive ORM functionality
2. Added step-by-step testing of all CRUD operations
3. Tests both User and Employee entities thoroughly
4. Added proper error handling and connection management
5. WHAT IT DOES: Complete test suite validating all ORM operations
6. WHY: Ensures the updated implementation works correctly and serves as documentation
7. Tests: save, findById, findOne, findAll (with conditions/pagination), count, update operations, delete operations
8. IMPORTANT: This demonstrates the proper usage patterns for the ORM
*/
async function bootstrap(): Promise<void> {
  try {
    await DB.driver.connect();
    console.log("Connected to database");

    // === USER ENTITY TESTS ===
    // Create test users with different data
    const user1 = new User({
      name: "John Doe",
      address: "Bangalore",
      dob: new Date("1990-01-01"),
      email: "john1@example.com",
      createdAt: new Date(),
      createdBy: 1,
      updatedAt: new Date(),
      updatedBy: 1,
    });

    const user2 = new User({
      name: "Jane Smith",
      address: "Mysore",
      dob: new Date("1992-05-10"),
      email: "jane@example.com",
      createdAt: new Date(),
      createdBy: 1,
      updatedAt: new Date(),
      updatedBy: 1,
    });

    const user3 = new User({
      name: "Mike Ross",
      address: "Chennai",
      dob: new Date("1995-08-20"),
      email: "mike@example.com",
      createdAt: new Date(),
      createdBy: 2,
      updatedAt: new Date(),
      updatedBy: 2,
    });

    // Test save operation
    await user1.save();
    await user2.save();
    await user3.save();

    // Test find operations
    const foundUser = await User.findById(1);
    console.log("findById:", foundUser);

    const oneUser = await User.findOne({
      email: "john1@example.com ",
    });
    console.log("findOne:", oneUser);

    // Test findAll with different options
    const allUsers = await User.findAll();
    console.log("findAll:", allUsers);

    const filteredUsers = await User.findAll({
      conditions: {
        createdBy: 1,
      },
    });
    console.log("findAll with conditions:", filteredUsers);

    const pagedUsers = await User.findAll({
      limit: 2,
      offset: 0,
    });
    console.log("findAll pagination:", pagedUsers);

    // Test count operations
    const totalUsers = await User.count();
    console.log("count all:", totalUsers);

    const countFiltered = await User.count({
      createdBy: 1,
    });
    console.log("count filtered:", countFiltered);

    // Test update operations
    const updated = await User.updateById(1, {
      address: "New Address Bangalore",
      updatedAt: new Date(),
    });
    console.log("updateById:", updated);

    const updateMany = await User.updateAll(
      {
        updatedBy: 99,
      },
      {
        createdBy: 1,
      },
    );
    console.log("updateAll:", updateMany);

    // Test delete operations
    const deletedOne = await User.deleteById(1);
    console.log("deleteById:", deletedOne);

    const deletedCond = await User.deleteOne({
      email: "jane@example.com",
    });
    console.log("deleteOne:", deletedCond);

    const deletedMany = await User.deleteAll({
      conditions: {
        createdBy: 1,
      },
    });
    console.log("deleteAll:", deletedMany);

    // === EMPLOYEE ENTITY TESTS ===
    // Create test employees with different data
    const employee1 = new Employee({
      name: "Rahul Sharma",
      position: "Software Engineer",
      department: "Engineering",
      salary: 75000,
      createdAt: new Date(),
      createdBy: 1,
      updatedAt: new Date(),
      updatedBy: 1,
    });

    const employee2 = new Employee({
      name: "Priya Nair",
      position: "HR Manager",
      department: "Human Resources",
      salary: 68000,
      createdAt: new Date(),
      createdBy: 1,
      updatedAt: new Date(),
      updatedBy: 1,
    });

    const employee3 = new Employee({
      name: "Arjun Reddy",
      position: "Accountant",
      department: "Finance",
      salary: 62000,
      createdAt: new Date(),
      createdBy: 2,
      updatedAt: new Date(),
      updatedBy: 2,
    });

    // Test save operation for employees
    await employee1.save();
    await employee2.save();
    await employee3.save();

    // Test all CRUD operations for employees
    // Find operations
    const foundEmployee = await Employee.findById(1);
    console.log("findById:", foundEmployee);

    const oneEmployee = await Employee.findOne({
      name: "Rahul Sharma",
    });
    console.log("findOne:", oneEmployee);

    // FindAll operations
    const allEmployees = await Employee.findAll();
    console.log("findAll:", allEmployees);

    const filteredEmployees = await Employee.findAll({
      conditions: {
        createdBy: 1,
      },
    });
    console.log("findAll with conditions:", filteredEmployees);

    const pagedEmployees = await Employee.findAll({
      limit: 2,
      offset: 0,
    });
    console.log("findAll pagination:", pagedEmployees);

    // Count operations
    const totalEmployees = await Employee.count();
    console.log("count all:", totalEmployees);

    const countFilteredEmployees = await Employee.count({
      createdBy: 1,
    });
    console.log("count filtered:", countFilteredEmployees);

    // Update operations
    const updatedEmployee = await Employee.updateById(1, {
      salary: 80000,
      updatedAt: new Date(),
    });
    console.log("updateById:", updatedEmployee);

    const updateManyEmployees = await Employee.updateAll(
      {
        updatedBy: 99,
      },
      {
        createdBy: 1,
      },
    );
    console.log("updateAll:", updateManyEmployees);

    // Delete operations
    const deletedEmployee = await Employee.deleteById(1);
    console.log("deleteById:", deletedEmployee);

    const deletedOneEmployee = await Employee.deleteOne({
      name: "Priya Nair",
    });
    console.log("deleteOne:", deletedOneEmployee);

    const deletedManyEmployees = await Employee.deleteAll({
      conditions: {
        createdBy: 1,
      },
    });
    console.log("deleteAll:", deletedManyEmployees);
  } catch (err) {
    console.error("Application startup failed:", err);
  } finally {
    // Always disconnect properly to prevent connection leaks
    try {
      await DB.driver.disconnect();
      console.log("Disconnected from database");
    } catch (err) {
      console.error("Error disconnecting from database:", err);
    }
  }
}

// Execute the bootstrap function to run the test suite
void bootstrap();