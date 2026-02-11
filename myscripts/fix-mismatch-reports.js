const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { DocumentStore, GetAttachmentOperation } = require('ravendb');

// --- Configuration ---
const CONCURRENT_REQUESTS = 16;

// --- Argument Parsing ---
const [targetDir, ravenUrl, dbName] = process.argv.slice(2);

if (!targetDir || !ravenUrl || !dbName) {
    console.error('Error: Missing required arguments.');
    console.error('Usage: node fix-mismatch-reports.js <path-to-parts-dir> <raven-db-url> <database-name>');
    process.exit(1);
}

// --- RavenDB Setup ---
const store = new DocumentStore(ravenUrl, dbName);
store.initialize();

async function processFile(filePath) {
    console.log(`\nProcessing file: ${path.basename(filePath)}`);
    
    // Read the entire file line by line to handle broken CSV structure manually
    const lines = [];
    try {
        const fileStream = fs.createReadStream(filePath);
        const rl = readline.createInterface({
            input: fileStream,
            crlfDelay: Infinity
        });

        for await (const line of rl) {
            lines.push(line);
        }
    } catch (err) {
        console.error(`Failed to read ${filePath}:`, err);
        return;
    }

    if (lines.length === 0) return;

    // We assume the first line is header, but we won't rely on it for column indices
    // due to the shifted column issue in data rows.
    
    const tasks = [];
    let mismatchCount = 0;

    // Helper to safely remove surrounding quotes
    const unquote = (str) => str ? str.trim().replace(/^"|"$/g, '') : '';
    // Helper to intelligently split CSV line respecting quotes
    const splitCsv = (line) => line.split(/,(?=(?:(?:[^"]*"){2})*[^"]*$)/);

    // Scan for mismatches
    // Skip header (index 0)
    for (let i = 1; i < lines.length; i++) {
        const line = lines[i];
        const cols = splitCsv(line);
        
        // Because fields like ChangeVector might contain unquoted commas, the row length varies.
        // However, Status is always 2nd to last, and ErrorDetails is last.
        if (cols.length < 3) continue;

        const statusColIndex = cols.length - 2;
        const status = unquote(cols[statusColIndex]);

        // Process 'MISMATCH' rows where we presumably haven't fixed the message yet (check for Diff string)
        // Or just process all mismatches to be safe.
        if (status === 'MISMATCH') {
            mismatchCount++;
            
            // Construct a row object for the valid fields we can rely on (beginning of line)
            const rowData = {
                DocumentId: unquote(cols[0]),
                Name: unquote(cols[1]), 
                Size: unquote(cols[2]),
                Hash: unquote(cols[3]),
                Type: unquote(cols[4]),
                // Try to reconstruct ChangeVector from the middle if needed, or rely on logic
                 // If Type is 'Revision', we need ChangeVector. It's everything between Type and Etag/Status.
                ChangeVectorParts: cols.slice(5, cols.length - 2), // Rough capture
                ErrorDetailsIndex: cols.length - 1
            };
            
            tasks.push({ lineIndex: i, rowData, existingCols: cols });
        }
    }

    if (mismatchCount === 0) {
        console.log(`  - No mismatches found. Skipping.`);
        return;
    }

    console.log(`  - Found ${mismatchCount} mismatches. Fetching data from RavenDB...`);

    // Worker pool parameters
    let completed = 0;
    
    const processTask = async (task) => {
        const { rowData, existingCols } = task;
        const docId = rowData.DocumentId;
        const name = rowData.Name;
        const csvSize = parseInt(rowData.Size, 10);
        
        const csvType = rowData.Type;
        
        // Reconstruct Change Vector if it was split
        let changeVectorRaw = "";
        const cvEndIndex = existingCols.length - 3; // Index before verification status (Etag)
        
        if (cvEndIndex >= 5) {
             const cvParts = existingCols.slice(5, existingCols.length - 2); // CV parts + Etag
             if (cvParts.length > 0) {
                 // The last part is likely Etag. Remove it.
                 cvParts.pop(); 
                 // Fix: preserve original spacing, do not trim!
                 changeVectorRaw = cvParts.join(','); 
             }
        }

        let operationType = (csvType && csvType.toLowerCase() === 'revision') ? 'Revision' : 'Document';
        let vectorForAPI = (operationType === 'Revision') ? unquote(changeVectorRaw) : null;

        // Default to keeping existing details, but we will overwrite usually
        let errorDetails = unquote(existingCols[rowData.ErrorDetailsIndex]);

        try {
            const operation = new GetAttachmentOperation(docId, name, operationType, vectorForAPI);
            const result = await store.operations.send(operation);

            if (result) {
                if (result.data) result.data.destroy();

                const mismatches = [];
                const dbSize = result.details.size;

                if (csvSize !== dbSize) {
                    const diff = Math.abs(dbSize - csvSize);
                    mismatches.push(`SIZE_MISMATCH (DB: ${dbSize}, CSV: ${csvSize}, Diff: ${diff})`);
                }
                
                if (rowData.Hash && rowData.Hash !== result.details.hash) {
                    mismatches.push('HASH_MISMATCH');
                }

                if (mismatches.length > 0) {
                    errorDetails = mismatches.join('; ');
                } else {
                    // Important: If it matches now, clear the error details or mark as spurious
                    errorDetails = "MATCHED_ON_RECHECK";
                }
            } else {
                 // Important: Handle NOT FOUND explicitly
                 errorDetails = "NOT_FOUND_ON_RECHECK"; 
            }
        } catch (err) {
             errorDetails = `RECHECK_FAILED: ${err.message}`;
        }

        // Update the specific column in memory
        existingCols[rowData.ErrorDetailsIndex] = `"${errorDetails}"`;
        lines[task.lineIndex] = existingCols.join(',');
        
        completed++;
        if (completed % 100 === 0) process.stdout.write('.');
    };

    // Execute batches
    for (let i = 0; i < tasks.length; i += CONCURRENT_REQUESTS) {
        const batch = tasks.slice(i, i + CONCURRENT_REQUESTS);
        await Promise.all(batch.map(processTask));
    }

    console.log(`\n  - Writing updated file...`);
    fs.writeFileSync(filePath, lines.join('\n'));
    console.log(`  - Done.`);
}

async function main() {
    try {
        const absTargetDir = path.isAbsolute(targetDir) ? targetDir : path.join(__dirname, targetDir);
        
        if (!fs.existsSync(absTargetDir)) {
            console.error(`Target directory not found: ${absTargetDir}`);
            return;
        }

        const files = fs.readdirSync(absTargetDir).filter(f => f.endsWith('-verified.csv'));
        console.log(`Found ${files.length} verified files in ${absTargetDir}`);

        for (const file of files) {
            await processFile(path.join(absTargetDir, file));
        }

        console.log('\nAll files processed.');

    } catch (err) {
        console.error('Fatal error:', err);
    } finally {
        store.dispose();
    }
}

main();
