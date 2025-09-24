import admin from "firebase-admin";

const serviceAccount = await Bun.file("peec-prod-firestore-key.json").json();

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
  });
}

export const db = admin.firestore();
