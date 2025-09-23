import admin from "firebase-admin";

// Initialize Firebase Admin SDK
// Make sure you have the service account key file in your project
const serviceAccount = await Bun.file("peec-prod-firestore-key.json").json();

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();

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
