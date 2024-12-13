const cron = require("node-cron");
const lastModifiedListener = require("./lastModifiedListener.js").lastModifiedListener; // Use destructuring if using named export

// Schedule the cron job to run every 5 minutes
cron.schedule("*/5 * * * *", async () => {
    try {
        console.log("Executing PostgreSQL last modified listener job...");
        await lastModifiedListener();
        console.log("PostgreSQL last modified listener job executed successfully.");
    } catch (error) {
        console.error("Error executing PostgreSQL last modified listener job:", error.message);
    }
});

console.log("PostgreSQL Last Modified Listener is running...");
