import { db } from "./firebase";
import admin from "firebase-admin";

export async function fetchCompanies() {
  console.log("Connecting to Firestore to fetch all companies...");

  try {
    const companiesCollection = db.collection("companies");
    const snapshot = await companiesCollection.get();

    if (snapshot.empty) {
      console.log("No matching documents.");
      return;
    }

    const companies: any[] = [];
    snapshot.forEach((doc) => {
      companies.push({ id: doc.id, ...doc.data() });
    });

    console.log(`Successfully fetched ${companies.length} companies.`);

    const outputDir = "data";
    const outputFilename = "firestore_companies.json";
    await Bun.write(
      `${outputDir}/${outputFilename}`,
      JSON.stringify(companies, null, 2)
    );

    console.log(`✅ Data saved to ${outputFilename}`);
  } catch (error) {
    console.error("Error fetching companies:", error);
  }
}

export async function fetchOrganizations() {
  console.log("Connecting to Firestore to fetch all organizations...");

  const outputDir = "data";
  const outputFilename = "firestore_organizations.json";
  const outputFile = `${outputDir}/${outputFilename}`;
  const writer = Bun.file(outputFile).writer();
  writer.write("[\n");

  try {
    let lastDoc: admin.firestore.QueryDocumentSnapshot | undefined = undefined;
    let page = 1;
    let totalFetched = 0;
    const pageSize = 100;
    let isFirst = true;

    const organizationsCollection = db
      .collection("organizations")
      .where("status", "in", ["CUSTOMER", "TRIAL"])
      .where("isDeleted", "==", false);
    const snapshot = await organizationsCollection.count().get();
    const count = snapshot.data().count;
    console.log(`Successfully counted ${count} organizations.`);

    while (true) {
      console.log(`Fetching page ${page}...`);

      let query = db
        .collection("organizations")
        .where("status", "in", ["CUSTOMER", "TRIAL"])
        .where("isDeleted", "==", false)
        .orderBy(admin.firestore.FieldPath.documentId())
        .limit(pageSize);

      if (lastDoc) {
        query = query.startAfter(lastDoc);
      }

      const orgSnapshot = await query.get();

      if (orgSnapshot.empty) {
        console.log("No more organizations found.");
        break;
      }

      for (const orgDoc of orgSnapshot.docs) {
        const promptsQuery = orgDoc.ref
          .collection("prompts")
          .where("isActive", "==", true)
          .where("isDeleted", "==", false);

        const promptsCountSnapshot = await promptsQuery.count().get();
        const promptsCount = promptsCountSnapshot.data().count;

        const orgDocData = orgDoc.data();
        delete orgDocData.tags;

        const orgData = {
          id: orgDoc.id,
          ...orgDocData,
          promptsCount,
        };

        if (!isFirst) {
          writer.write(",\n");
        }
        writer.write(JSON.stringify(orgData, null, 2));
        isFirst = false;
      }

      totalFetched += orgSnapshot.size;
      console.log(
        `Fetched ${orgSnapshot.size} orgs. Total fetched: ${totalFetched}`
      );

      lastDoc = orgSnapshot.docs[orgSnapshot.docs.length - 1];
      page++;
    }

    writer.write("\n]\n");
    writer.end();

    console.log(`✅ Data saved to ${outputFilename}`);
  } catch (error) {
    console.error("Error fetching organizations:", error);
    writer.end();
  }
}
