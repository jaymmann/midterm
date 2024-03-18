const { Storage } = require('@google-cloud/storage');
const csvParser = require('csv-parser');
const storage = new Storage();

// Updated source bucket for the sample PDFs
const sourceBucketName = 'iupui-cit41200-class-malpdf-pdf-source';
const csvBucketName = 'cit41200-jaymmann-pdfmalware-sample'; // Bucket where the CSV is located
const csvFileName = 'PDFMalware2022.csv'; // CSV file name

async function getClassificationsFromCSV() {
  const classifications = {};
  return new Promise((resolve, reject) => {
    storage.bucket(csvBucketName).file(csvFileName).createReadStream()
      .pipe(csvParser())
      .on('data', (data) => {
        classifications[data.filename] = data.Class;
      })
      .on('end', () => {
        resolve(classifications);
      })
      .on('error', reject);
  });
}

let sortedCounter = 0; // Counter for the number of PDFs sorted

async function checkAndMovePDF(filename, classification) {
  const exists = await storage.bucket(sourceBucketName).file(filename).exists();
  if (exists[0]) {
    const destinationBucketName = classification === 'Benign' ? 'cit41200-jaymmann-benign-pdf' : 'cit41200-jaymmann-malicious-pdf';
    try {
      await storage.bucket(sourceBucketName).file(filename).copy(storage.bucket(destinationBucketName).file(filename));
      console.log(`Copied ${filename} to ${destinationBucketName}`);
      sortedCounter++; // Increment the counter each time a file is successfully sorted
    } catch (error) {
      console.error(`Failed to move ${filename}:`, error.message);
    }
  } else {
    //console.log(`File does not exist: ${filename}`);
  }
}

async function classifyAndMoveSamplePDFs() {
  try {
    const classifications = await getClassificationsFromCSV();
    for (const [filename, classification] of Object.entries(classifications)) {
      await checkAndMovePDF(filename, classification);
    }
    // After all PDFs are processed, log the total number sorted
    console.log(`Total PDFs sorted: ${sortedCounter}`);
  } catch (error) {
    console.error('Failed to classify and move sample PDFs:', error);
  }
}

classifyAndMoveSamplePDFs();
