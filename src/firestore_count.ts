import { db } from "./firebase";

export async function countOrganizations() {
  console.log("Connecting to Firestore to count organizations...");

  try {
    const organizationsCollection = db.collection("organizations");
    const snapshot = await organizationsCollection.count().get();
    const count = snapshot.data().count;

    console.log(`Successfully counted ${count} organizations.`);

    return count;
  } catch (error) {
    console.error("Error counting organizations:", error);
    return 0;
  }
}
