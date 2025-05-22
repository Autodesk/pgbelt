const { Client } = require("pg");

// Configure the PostgreSQL client
const client = new Client({
  host: "localhost",
  port: 5432,
  user: "user1", // Replace with your PostgreSQL username
  password: "password1", // Replace with your PostgreSQL password
  database: "postgres", // Replace with your PostgreSQL database name
});

const client2 = new Client({
  host: "localhost",
  port: 5433,
  user: "user2", // Replace with your PostgreSQL username
  password: "password2", // Replace with your PostgreSQL password
  database: "postgres", // Replace with your PostgreSQL database name
});

// Function to insert records
async function insertRecords(pgClient, numRecords) {
  try {
    await pgClient.connect();
    console.log("Connected to the database");

    for (let i = 1; i <= numRecords; i++) {
      const name = `Product ${i}`;
      const type = `Type ${String.fromCharCode(65 + (i % 3))}`; // Cycles through Type A, Type B, Type C

      const query = "INSERT INTO products (name, type) VALUES ($1, $2)";
      const values = [name, type];

      await pgClient.query(query, values);
      console.log(`Inserted: ${name}, ${type}`);
    }

    console.log(
      `Successfully inserted ${numRecords} records into the products table`
    );
  } catch (err) {
    console.error("Error inserting records:", err);
  } finally {
    await pgClient.end();
    console.log("Disconnected from the database");
  }
}

// Number of records to insert
const numRecords = 182679498; // You can change this value to insert more or fewer records

// Insert records
insertRecords(client, numRecords);
insertRecords(client2, numRecords);
