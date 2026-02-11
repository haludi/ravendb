const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { DocumentStore, GetAttachmentOperation } = require('ravendb');
const csv = require('csv-parser');

// --- Configuration Constants ---
const MAX_SPLIT_SIZE_BYTES = 4 * 1024 * 1024; // 4 MB
const SPLIT_DIR_NAME = 'split-csv-parts'; // This is overridden in main()
const CONCURRENT_REQUESTS = 16;

// --- 1. Argument Parsing ---
const [csvPath, ravenUrl, dbName] = process.argv.slice(2);

if (!csvPath || !ravenUrl || !dbName) {
    console.error('Error: Missing required arguments.');
    console.error('Usage: node verify-attachments.js <path-to-csv> <raven-db-url> <database-name>');
    process.exit(1);
}

// --- 2. RavenDB Store Initialization ---
const store = new DocumentStore(ravenUrl, dbName);
store.initialize();

// --- 3. File Splitting Logic (Updated with Correct File Filtering) ---
async function splitCsvFile(inputPath, splitDir) {
    // 3a. Check if split already exists
    if (fs.existsSync(splitDir)) {
        // --- FIX: Use a precise regex to get ONLY source part files ---
        const existingFiles = fs.readdirSync(splitDir)
            .filter(f => /^part_\d{3}\.csv$/.test(f))
            .sort(); // Ensure 001, 002, 003 order

        if (existingFiles.length > 0) {
            console.log(`\nDirectory '${splitDir}' exists and contains ${existingFiles.length} source part files.`);
            console.log('Skipping split process and using existing files.');
            return existingFiles.map(f => path.join(splitDir, f));
        }
    }

    // 3b. Perform Split (Only if folder missing or empty)
    console.log(`\nStarting split process for: ${inputPath}`);

    if (!fs.existsSync(splitDir)) {
        fs.mkdirSync(splitDir, { recursive: true });
        console.log(`Created directory: ${splitDir}`);
    }

    const fileStream = fs.createReadStream(inputPath);
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    let header = '';
    let currentSplitFileIndex = 0;
    let currentSplitFileSize = 0;
    let currentWriteStream = null;
    const splitFilePaths = [];
    let isHeaderRead = false;

    const startNewSplitFile = () => {
        if (currentWriteStream) currentWriteStream.end();
        currentSplitFileIndex++;
        currentSplitFileSize = 0;

        const fileName = `part_${currentSplitFileIndex.toString().padStart(3, '0')}.csv`;
        const filePath = path.join(splitDir, fileName);
        currentWriteStream = fs.createWriteStream(filePath);
        splitFilePaths.push(filePath);

        if (header) {
            currentWriteStream.write(header);
            currentSplitFileSize += Buffer.byteLength(header, 'utf8');
        }
    };

    for await (const line of rl) {
        const lineWithNewline = line + '\n';
        const lineSizeBytes = Buffer.byteLength(lineWithNewline, 'utf8');

        if (!isHeaderRead) {
            header = lineWithNewline;
            isHeaderRead = true;
            startNewSplitFile();
            continue;
        }

        if (currentSplitFileSize + lineSizeBytes > MAX_SPLIT_SIZE_BYTES) {
            startNewSplitFile();
        }

        currentWriteStream.write(lineWithNewline);
        currentSplitFileSize += lineSizeBytes;
    }

    if (currentWriteStream) currentWriteStream.end();

    console.log(`\nSplit complete. Total parts created: ${currentSplitFileIndex}`);
    return splitFilePaths;
}

// --- 4. Verification Core Logic (Updated with Robust Concurrency) ---
async function processVerification(splitFilePath) {
    const splitFileName = path.basename(splitFilePath, '.csv');
    const outputFileName = `${splitFileName}-verified.csv`;
    const outputCsvPath = path.join(path.dirname(splitFilePath), outputFileName);
    const writer = fs.createWriteStream(outputCsvPath);

    console.log(`\n--- Verifying Part: ${splitFileName} ---`);
    console.log(`Report will be saved to: ${outputCsvPath}`);

    const summary = { total: 0, ok: 0, notFound: 0, errors: 0, mismatched: { count: 0 } };

    try {
        const results = [];
        await new Promise((resolve, reject) => {
            fs.createReadStream(splitFilePath)
                .pipe(csv({ mapHeaders: ({ header }) => header.trim() }))
                .on('data', (data) => results.push(data))
                .on('end', resolve)
                .on('error', reject);
        });

        // Write header immediately after reading
        if (results.length > 0) {
            const headers = Object.keys(results[0]);
            writer.write(headers.join(',') + ',VerificationStatus,ErrorDetails\n');
        } else {
            console.log(`File ${splitFileName} is empty. Skipping.`);
            return; // Exit if the file part is empty
        }

        // Helper to properly quote CSV values
        const toCsvVal = (val) => {
            const s = (val === null || val === undefined) ? '' : String(val);
            if (s.includes(',') || s.includes('"') || s.includes('\n') || s.includes('\r')) {
                return `"${s.replace(/"/g, '""')}"`;
            }
            return s;
        };

        // --- FIX: Robust Worker Pool for Concurrency ---
        const taskQueue = [...results.entries()]; // Use entries to get index for line number
        let processedResults = new Array(results.length);

        const worker = async () => {
            while (taskQueue.length > 0) {
                const [index, row] = taskQueue.shift();
                if (!row) continue;

                const rowNumberInPart = index + 1;
                const docId = row.DocumentId;
                const name = row.Name;

                if (!docId || !name) {
                    throw new Error(`Invalid data: Missing DocumentId or Name on CSV Line # ${rowNumberInPart} in file ${splitFileName}. Aborting.`);
                }

                let verificationStatus = 'UNKNOWN';
                let errorDetails = '';

                try {
                    const csvSize = parseInt(row.Size, 10);
                    const csvHash = row.Hash;
                    const csvType = row.Type;
                    const changeVectorRaw = row.ChangeVector;
                    let operationType = (csvType && csvType.toLowerCase() === 'revision') ? 'Revision' : 'Document';
                    let vectorForAPI = (operationType === 'Revision') ? changeVectorRaw : null;

                    const operation = new GetAttachmentOperation(docId, name, operationType, vectorForAPI);
                    const result = await store.operations.send(operation);

                    if (result ) {
                        if(result.data){
                            result.data.destroy();
                        }
                        else {
                            console.log(`not stream`);
                        }
                    }

                    if (!result) {
                        verificationStatus = 'NOT_FOUND';
                    } else {
                        const mismatches = [];
                        const dbSize = result.details.size;

                        if (csvSize !== dbSize) {
                            const diff = Math.abs(dbSize - csvSize);
                            mismatches.push(`SIZE_MISMATCH (DB: ${dbSize}, CSV: ${csvSize}, Diff: ${diff})`);
                        }
                        
                        if (csvHash !== result.details.hash) mismatches.push('HASH_MISMATCH');

                        if (mismatches.length === 0) {
                            verificationStatus = 'OK';
                        } else {
                            verificationStatus = 'MISMATCH';
                            errorDetails = mismatches.join('; ');
                        }
                    }
                } catch (err) {
                    errorDetails = err.message || err.toString();
                    throw new Error(`RavenDB Operation Failed on ${docId} (Line ${rowNumberInPart}): ${errorDetails}`);
                }

                processedResults[index] = { row, verificationStatus, errorDetails };
            }
        };

        const workers = Array(CONCURRENT_REQUESTS).fill(0).map(worker);
        await Promise.all(workers); // Will fail fast on the first error

        // Write results and calculate summary
        for(const item of processedResults) {
            summary.total++;
            if (item.verificationStatus === 'OK') summary.ok++;
            else if (item.verificationStatus === 'NOT_FOUND') summary.notFound++;
            else if (item.verificationStatus === 'MISMATCH') summary.mismatched.count++;

            // Reconstruct row with proper quoting to avoid broken CSVs
            const rowValues = Object.values(item.row).map(toCsvVal).join(',');
            writer.write(`${rowValues},"${item.verificationStatus}","${item.errorDetails}"\n`);
        }

        console.log(`\nVerification of ${splitFileName} complete.`);
        console.log(`TOTAL: ${summary.total} | OK: ${summary.ok} | NOT_FOUND: ${summary.notFound} | MISMATCHED: ${summary.mismatched.count}`);

    } catch (err) {
        console.error(`\nFATAL SCRIPT ERROR in file ${splitFileName}: Process Aborted.`);
        console.error(`- Error: ${err.message}`);
        throw err;
    } finally {
        writer.end();
    }
}

// --- 5. Main Control Logic ---
async function main() {
    try {
        let splitDirName = path.basename(csvPath, '.csv');
        const splitDir = path.join(path.dirname(csvPath), splitDirName);

        const splitFiles = await splitCsvFile(csvPath, splitDir);

        let startIndex = 0;
        let lastVerifiedFound = false;
        for (let i = 0; i < splitFiles.length; i++) {
            const partPath = splitFiles[i];
            const partName = path.basename(partPath, '.csv');
            const verifiedPath = path.join(splitDir, `${partName}-verified.csv`);

            if (fs.existsSync(verifiedPath)) {
                startIndex = i;
                lastVerifiedFound = true;
            } else {
                break;
            }
        }

        if (lastVerifiedFound) {
            console.log(`\n>>> RESUMING LOGIC DETECTED <<<`);
            console.log(`Restarting from file index ${startIndex}: ${path.basename(splitFiles[startIndex])}`);
        } else {
            console.log(`\n>>> STARTING FRESH <<<`);
        }

        for (let i = startIndex; i < splitFiles.length; i++) {
            await processVerification(splitFiles[i]);
        }

        console.log('\n======================================');
        console.log('  ALL FILES PROCESSED SUCCESSFULLY!');
        console.log(`  Reports are in the ${splitDirName} directory.`);
        console.log('======================================');

    } catch (err) {
        console.log('\n--- Script execution stopped due to a fatal error. ---');
    } finally {
        store.dispose();
    }
}

main();
