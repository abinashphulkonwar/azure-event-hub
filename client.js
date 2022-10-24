const {
  EventHubConsumerClient,
  earliestEventPosition,
} = require("@azure/event-hubs");
require("dotenv").config();
const connectionString = process.env.CONNECTION_ENDPOINT;
const eventHubName = process.env.EVENTHUBNAME;
const consumerGroup = process.env.CONSUMERGROUP;

async function main() {
  // Create a consumer client for the event hub by specifying the checkpoint store.
  const consumerClient = new EventHubConsumerClient(
    consumerGroup,
    connectionString,
    eventHubName
  );

  // Subscribe to the events, and specify handlers for processing the events and errors.
  const subscription = consumerClient.subscribe(
    {
      processEvents: async (events, context) => {
        if (events.length === 0) {
          console.log(
            `No events received within wait time. Waiting for next interval`
          );
          return;
        }
        console.log(events, context);

        await context.updateCheckpoint(events[events.length - 1]);
      },

      processError: async (err, context) => {
        console.log(`Error : ${err}`);
      },
    },
    { startPosition: earliestEventPosition }
  );
  // After 30 seconds, stop processing.
  await new Promise((resolve) => {
    setTimeout(async () => {
      await subscription.close();
      await consumerClient.close();
      resolve();
    }, 30000);
  });
}

main().catch((err) => {
  console.log("Error occurred: ", err);
});
