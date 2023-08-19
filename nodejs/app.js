// https://blog.logrocket.com/crud-rest-api-node-js-express-postgresql/

const express = require('express');
const Pool = require('pg').Pool
const csvParser = require('csv-parser');
const { Readable } = require('stream');
const multer = require('multer');

const app = express();
const PORT = 5000;

const pool = new Pool({
    user: process.env.NODEJS_USER,
    host: 'timescaledb',
    database: process.env.POSTGRES_DB,
    password: process.env.NODEJS_PASSWORD,
    port: 5432,
})

// Middleware for JSON parsing
app.use(express.json());

// Middleware for uploading files
const upload = multer();

// Endpoint for uploading images
app.post('/upload_screenshot', upload.single('file'), async (req, res) => {
    const ip_address = req.headers['x-forwarded-for'] || req.socket.remoteAddress;

    // Parse filename
    const filename = req.file?.originalname;
    if (!filename) {
        return handleError(res, 400, 'Missing file');
    }

    const regex = /^(?<receiver>[A-Za-z][A-Za-z0-9]{2,8})_(?<freq>\d+\.\d+)MHz_(?<wtf>\d+\.\d+)Msps_(?<epoch>\d+)sec\.jpg$/;
    const match = regex.exec(filename);

    if (!match) {
        return handleError(res, 400, 'Invalid filename format');
    }

    const receiver = match?.groups?.receiver;
    const freq = parseFloat(match?.groups?.freq);
    const wtf = parseFloat(match?.groups?.wtf);
    const timestamp_event = new Date(parseInt(match?.groups?.epoch) * 1000);

    const data = req.file.buffer;
 
    const gain = 0; // where to get from?

    // Spam prevention
    const ipEntryLimit = 10;
    const ipEntryCount = await getCountBy('ip_address', ip_address);
    if (ipEntryCount >= ipEntryLimit) {
        return handleError(res, 400, `Upload limit for today (${ipEntryLimit}) for IP address ${ip_address} reached`);
    }

    const receiverEntryLimit = 5;
    const receiverEntryCount = await getCountBy('receiver', receiver);
    if (receiverEntryCount >= receiverEntryLimit) {
        return handleError(res, 400, `Upload limit for today (${receiverEntryLimit}) for receiver ${receiver} reached`);
    };

    // Check if receiver exists
    const receiverExistance = await getReceiverExistance(receiver);
    if (!receiverExistance) {
        return handleError(res, 400, `Receiver ${receiver} does not exist.`)
    }

    await insertScreenshot(ip_address, receiver, timestamp_event, gain, data);
    res.status(200).json({ message: 'File uploaded successfully' });
});

// Endpoint for uploading measurements
app.post('/upload_measurement', upload.single('file'), async (req, res) => {
    const ip_address = req.headers['x-forwarded-for'] || req.socket.remoteAddress;

    // Parse filename
    const filename = req.file?.originalname;
    if (!filename) {
        return handleError(res, 400, 'Missing file');
    }
    const regex = /^(?<receiver>[A-Za-z][A-Za-z0-9]{2,8})_g(?<gain>\d{1,2}\.\d)_(?<epoch>\d+)sec\.csv$/;
    const match = regex.exec(filename);

    if (!match) {
        return handleError(res, 400, 'Invalid filename format');
    }

    const receiver = match?.groups?.receiver;
    const gain = parseFloat(match?.groups?.gain);
    const timestamp_event = new Date(parseInt(match?.groups?.epoch) * 1000);

    // Spam prevention
    const ipEntryLimit = 10;
    const ipEntryCount = await getCountBy('ip_address', ip_address);
    if (ipEntryCount >= ipEntryLimit) {
        return handleError(res, 400, `Upload limit for today (${ipEntryLimit}) for IP address ${ip_address} reached`);
    }

    const receiverEntryLimit = 5;
    const receiverEntryCount = await getCountBy('receiver', receiver);
    if (receiverEntryCount >= receiverEntryLimit) {
        return handleError(res, 400, `Upload limit for today (${receiverEntryLimit}) for receiver ${receiver} reached`);
    };

    // Check if receiver exists
    const receiverExistance = await getReceiverExistance(receiver);
    if (!receiverExistance) {
        return handleError(res, 400, `Receiver ${receiver} does not exist.`)
    }

    // Parse CSV data
    const data = req.file.buffer.toString();
    const freq = [];
    const power = [];
    const csvStream = new Readable({ read() { this.push(data); this.push(null); } });

    csvStream.pipe(csvParser({ headers: ['date', 'time', 'Hz low', 'Hz high', 'Hz step', 'samples', 'dbm'] }))
        .on('data', (row) => { freq.push(parseFloat(row['Hz low'])); power.push(parseFloat(row['dbm'])); })
        .on('end', async () => {
            await insertMeasurement(ip_address, receiver, timestamp_event, gain, freq, power);

            res.status(200).json({ message: 'File uploaded successfully' });
        });
});

// Fallback for routes not found
app.use((req, res) => {
    res.status(404).send('Not Found');
});

// Starte the server
app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});

// Database query for entries by column and value
function getCountBy(column, value) {
    return new Promise((resolve, reject) => {
        const query = `SELECT COUNT(*) as count FROM measurements WHERE timestamp_upload >= CURRENT_TIMESTAMP - INTERVAL'1 day' AND ${column} = '${value}'`;
        pool.query(query, [], (error, results) => {
            if (error) {
                reject(error);
            } else {
                resolve(results.rows[0].count);
            }
        });
    });
}

// Database query for receiver existance
function getReceiverExistance(receiver) {
    return new Promise((resolve, reject) => {
        const query = `SELECT COUNT(*) as count FROM receivers WHERE src_call = '${receiver}'`;
        pool.query(query, [], (error, results) => {
            if (error) {
                reject(error);                
            } else {
                resolve(results.rows[0].count != '0');
            }
        })
    })
}

// Inserting a screenshot into the database
function insertScreenshot(ip_address, receiver, timestamp_event, gain, data) {
    return new Promise((resolve, reject) => {
        const query = 'INSERT INTO screenshots (ip_address, receiver, timestamp_event, gain, data) VALUES ($1, $2, $3, $4, $5)';
        const values = [ip_address, receiver, timestamp_event, parseFloat(gain), data];

        pool.query(query, values, (error, results) => {
            if (error) {
                reject(error);
            } else {
                resolve();
            }
        });
    });
}

// Inserting a measurement into the database
function insertMeasurement(ip_address, receiver, timestamp_event, gain, freq, power) {
    return new Promise((resolve, reject) => {
        const query = 'INSERT INTO measurements (ip_address, receiver, timestamp_event, gain, data) VALUES ($1, $2, $3, $4, $5)';
        const values = [ip_address, receiver, timestamp_event, parseFloat(gain), [freq, power]];

        pool.query(query, values, (error, results) => {
            if (error) {
                reject(error);
            } else {
                resolve();
            }
        });
    });
}

// error handling function
function handleError(res, status, message) {
    res.status(status).json({ error: message });
}
