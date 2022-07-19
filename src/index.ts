import connectDB from "./config/db";
import { fetchAllUsers, getGrainData, manager } from "./utils";

const main = async (): Promise<void> => {
  await connectDB();
  const dbUsers = await fetchAllUsers();

  await manager.reloadLedger();
  const ledger = manager.ledger;

  const grainData = await getGrainData(ledger);

  console.log({ grainData });
  console.log({
    // ledger,   // in _accounts we get the info from sourcecred
    // dbUsers,  // we have the addresses here from database
    // grainData, // all grain distributed in here
    myself: ledger._accounts.get("SW3QLzfLRvQkXcmy4MxMNg"),
  });
};

main();
