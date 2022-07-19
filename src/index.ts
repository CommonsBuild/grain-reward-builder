import connectDB from "./config/db";
import { fetchAllUsers, getGrainData, manager } from "./utils";

const main = async (): Promise<void> => {
  await connectDB();
  const allUsers = await fetchAllUsers();

  await manager.reloadLedger();
  const ledger = manager.ledger;

  const grainData = await getGrainData(ledger);

  console.log({ grainData });
  console.log({
    // ledger,
    // allUsers,
    // grainData,
    myself: ledger._accounts.get("SW3QLzfLRvQkXcmy4MxMNg"),
  });
};

main();
