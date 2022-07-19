import { sourcecred } from "sourcecred";
import { config } from "dotenv";

import User, { IUser } from "./models/User";

config();

export const log = (message) => console.log(`${Date.now()}: ${message}`);

const storage = new sourcecred.ledger.storage.WritableGithubStorage({
  apiToken: process.env.GH_API_TOKEN,
  repo: process.env.REPO,
  branch: process.env.BRANCH,
});

export const manager = new sourcecred.ledger.manager.LedgerManager({ storage });

export const loadLedger = async () => {
  const ledger = await manager.reloadLedger();
  return ledger;
};

const loadCredGraph = async (): Promise<any> => {
  try {
    const instance = sourcecred.instance.readInstance.getNetworkReadInstance(
      process.env.REPO_AND_BRANCH
    );
    return instance.readCredGraph();
  } catch (err) {
    console.log(err);
    return null;
  }
};

export const fetchAllUsers = async (): Promise<IUser[]> => {
  const foundUsers = await User.find({});
  return foundUsers;
};

export const getGrainData = async (ledger) => {
  const credGraph = await loadCredGraph();
  if (!ledger || !credGraph) return;
  const intervals = credGraph
    .intervals()
    .map((i) => new Date(i.endTimeMs).toDateString())
    .reverse();
  return ledger
    .accounts()
    .map((a) => {
      const result = {
        id: a.identity.id,
        name: a.identity.name,
        totalGrainPaid: a.paid / 1000000000000000000,
      };

      // comment gpi to get latest paid, leave it to get all intervals
      const gpi = _calculateGrainEarnedPerInterval(a, credGraph.intervals())
        .reverse()
        .forEach((g, i) => (result[intervals[i]] = g / 1000000000000000000));
      return result;
    })
    .sort((a, b) => b.totalGrainPaid - a.totalGrainPaid);
};

const _calculateGrainEarnedPerInterval = (account, intervals) => {
  let allocationIndex = 0;
  return intervals.map((interval) => {
    let grain = sourcecred.ledger.grain.ZERO;
    while (
      account.allocationHistory.length - 1 >= allocationIndex &&
      interval.startTimeMs <
        account.allocationHistory[allocationIndex].credTimestampMs &&
      account.allocationHistory[allocationIndex].credTimestampMs <=
        interval.endTimeMs
    ) {
      grain = sourcecred.ledger.grain.add(
        grain,
        account.allocationHistory[allocationIndex].grainReceipt.amount
      );
      allocationIndex++;
    }
    return grain;
  });
};
