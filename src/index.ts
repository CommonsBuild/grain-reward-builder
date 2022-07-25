import connectDB from "./config/db";
import { fetchAllUsers, getGrainData, manager } from "./utils";

const main = async (): Promise<void> => {
  await connectDB();
  const dbUsers = await fetchAllUsers();

  await manager.reloadLedger();
  const ledger = manager.ledger;
  const ledgerAccounts = ledger._accounts;

  const grainData = await getGrainData(ledger);





  let jsonLedger = {};  
  ledgerAccounts.forEach((value, key) => {  
    jsonLedger[key] = value  
  });  
  console.log(jsonLedger)
  //console.log(typeof jsonLedger)



  let jsonUsers = {};  
  dbUsers.forEach((value, key) => {  
    jsonUsers[key] = value  
  });  
  let activeUsers = jsonUsers


  // console.log( dbUsers);
  // // console.log({
  // //   ledgerAccounts });
  // console.log({
  //   // ledger,   // in _accounts we get the info from sourcecred
  //   // dbUsers,  // we have the addresses here from database
  //   // grainData, // all grain distributed in here
  //   myself: ledger._accounts.get("SW3QLzfLRvQkXcmy4MxMNg"),
  // });



  const ObjectsToCsv = require('objects-to-csv');

// If you use "await", code must be inside an asynchronous function:
(async () => {

  const grainCsv = new ObjectsToCsv(grainData);
  await grainCsv.toDisk('./raw_data/grain.csv');

  const ledgerCsv = new ObjectsToCsv(Object.values(jsonLedger));
  await ledgerCsv.toDisk('./raw_data/ledgerAccounts.csv')


  const activatedCsv = new ObjectsToCsv(Object.values(activeUsers));
  await activatedCsv.toDisk('./raw_data/activatedUsers.csv')




})();
};

main();
