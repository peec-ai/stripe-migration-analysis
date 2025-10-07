import { db } from "./firebase";
import admin from "firebase-admin";
import { Query } from "firebase-admin/firestore";
import fs from "fs";
import path from "path";

const getLatestUpdateTimestamp = (
  data: any[]
): admin.firestore.Timestamp | null => {
  if (data.length === 0) {
    return null;
  }
  const latest = data.reduce((latest, current) => {
    const currentTimestamp = current.updatedAt?._seconds || 0;
    const latestTimestamp = latest.updatedAt?._seconds || 0;
    return currentTimestamp > latestTimestamp ? current : latest;
  });
  if (latest.updatedAt) {
    return admin.firestore.Timestamp.fromMillis(latest.updatedAt._seconds * 1000);
  }
  return null;
};

const mergeData = (existingData: any[], newData: any[]) => {
  const dataMap = new Map(existingData.map((item) => [item.id, item]));
  newData.forEach((item) => {
    dataMap.set(item.id, item);
  });
  return Array.from(dataMap.values());
};

const getCollectionData = async (
  collectionName: string,
  outputFile: string,
  baseQuery?: Query
) => {
  let query: Query = baseQuery || db.collection(collectionName);
  let existingData: any[] = [];
  const filePath = path.join(__dirname, "../../data", outputFile);

  if (fs.existsSync(filePath)) {
    console.log(`Incremental fetch for ${collectionName}...`);
    try {
      const fileContent = fs.readFileSync(filePath, "utf-8");
      existingData = JSON.parse(fileContent);
      const lastTimestamp = getLatestUpdateTimestamp(existingData);
      if (lastTimestamp) {
        console.log(
          `Fetching documents updated after ${lastTimestamp.toDate().toISOString()}`
        );
        query = query.where("updatedAt", ">", lastTimestamp);
      }
    } catch (e) {
      console.warn(`Could not parse ${outputFile}, doing a full fetch.`);
      existingData = [];
    }
  } else {
    console.log(`Full fetch for ${collectionName}...`);
  }

  const snapshot = await query.get();
  const newData = snapshot.docs.map((doc) => ({
    id: doc.id,
    ...doc.data(),
  }));

  console.log(
    `Fetched ${newData.length} new/updated documents from ${collectionName}.`
  );

  if (newData.length > 0) {
    const allData = mergeData(existingData, newData);
    fs.writeFileSync(filePath, JSON.stringify(allData, null, 2));
    console.log(`Wrote ${allData.length} total documents to ${outputFile}`);
  } else {
    console.log(`No new data for ${collectionName}.`);
  }
};

export async function fetchCompanies() {
  await getCollectionData("companies", "firestore_companies.json");
}

export async function fetchOrganizations() {
  console.log("Connecting to Firestore to fetch organizations...");
  const collectionName = "organizations";
  const outputFile = "firestore_organizations.json";
  const filePath = path.join(__dirname, "../../data", outputFile);

  let existingData: any[] = [];
  let query: Query = db
    .collection(collectionName)
    .where("status", "in", ["CUSTOMER", "TRIAL"])
    .where("isDeleted", "==", false);

  if (fs.existsSync(filePath)) {
    console.log(`Incremental fetch for ${collectionName}...`);
    try {
      const fileContent = fs.readFileSync(filePath, "utf-8");
      existingData = JSON.parse(fileContent);
      const lastTimestamp = getLatestUpdateTimestamp(existingData);
      if (lastTimestamp) {
        console.log(
          `Fetching documents updated after ${lastTimestamp.toDate().toISOString()}`
        );
        // Note: This query requires a composite index in Firestore on
        // 'status', 'isDeleted', and 'updatedAt'.
        query = query.where("updatedAt", ">", lastTimestamp);
      }
    } catch (e) {
      console.warn(`Could not parse ${outputFile}, doing a full fetch.`);
      existingData = [];
    }
  } else {
    console.log(`Full fetch for ${collectionName}...`);
  }

  try {
    const orgSnapshot = await query.get();

    if (orgSnapshot.empty) {
      console.log("No new or updated organizations found.");
      return;
    }

    console.log(`Found ${orgSnapshot.size} new/updated organizations.`);

    const newDataPromises = orgSnapshot.docs.map(async (orgDoc) => {
      const promptsQuery = orgDoc.ref
        .collection("prompts")
        .where("isActive", "==", true)
        .where("isDeleted", "==", false);

      const promptsCountSnapshot = await promptsQuery.count().get();
      const promptsCount = promptsCountSnapshot.data().count;

      const orgDocData = orgDoc.data();
      delete orgDocData.tags;

      return {
        id: orgDoc.id,
        ...orgDocData,
        promptsCount,
      };
    });

    const newData = await Promise.all(newDataPromises);

    console.log(
      `Successfully fetched ${newData.length} new/updated organizations with prompt counts.`
    );

    const allData = mergeData(existingData, newData);
    fs.writeFileSync(filePath, JSON.stringify(allData, null, 2));
    console.log(`✅ Wrote ${allData.length} total documents to ${outputFile}`);
  } catch (error) {
    console.error("Error fetching organizations:", error);
  }
}
