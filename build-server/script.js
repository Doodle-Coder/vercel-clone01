const { exec } = require('child_process')
const path = require('path')
const fs = require('fs')
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3')
const mime = require('mime-types');
const { Kafka } = require('kafkajs')



const s3Client = new S3Client({
    region:'ap-south-1',
    credentials: {
        accessKeyId:'AKIA6EB6EDAB6JPIQI5S',
        secretAccessKey:'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'
    }
})

const PROJECT_ID = process.env.PROJECT_ID
const DEPLOYMENT_ID = process.env.DEPLOYMENT_ID



const kafka = new Kafka({
    clientId:`docker-build-server-${DEPLOYMENT_ID}`,
    brokers: ['kafka-c4b1bad-jb673372-2b87.a.aivencloud.com:14086'],
    ssl:{
        ca: [fs.readFileSync(path.join(__dirname, 'kafka.pem'), 'utf-8')]
    },
    sasl:{
        username: 'avnadmin',
        password:'AVNS_to7PvZu0QJrWCewrxTv',
        mechanism: 'plain'
    }
})

const producer = kafka.producer()

async function publishLog(log) {
    await producer.send({topic: `container-logs`,messages:[{key:'log' ,value: JSON.stringify({PROJECT_ID, DEPLOYMENT_ID, log})}]})
}



async function init() {

    await producer.connect();
    console.log('Executing script.js')
    await publishLog('Build Started...')
    const outDirPath = path.join(__dirname, 'output')

    const p = exec(`cd ${outDirPath} && npm install && npm run build`)

    p.stdout.on('data', async function (data) {
        console.log(data.toString())
       await publishLog(data.toString())
    })

    p.stdout.on('error', async function (data) {
        console.log('Error', data.toString())
        await publishLog(`error: ${data.toString()}`)
    })

    p.on('close', async function () {
        console.log('Build Complete')
        await publishLog('Build Complete')
        const distFolderPath = path.join(__dirname, 'output', 'dist')
        const distFolderContents = fs.readdirSync(distFolderPath, { recursive: true })

       await publishLog(`Starting to upload`)
        for (const file of distFolderContents) {
            const filePath = path.join(distFolderPath, file)
            if (fs.lstatSync(filePath).isDirectory()) continue;

            console.log('uploading', filePath)
           await publishLog(`uploading ${file}`)

            const command = new PutObjectCommand({
                Bucket: 'vercel-clone-01',
                Key: `__outputs/${PROJECT_ID}/${file}`,
                Body: fs.createReadStream(filePath),
                ContentType: mime.lookup(filePath)
            })

            await s3Client.send(command)
            await publishLog(`uploaded ${file}`)
            console.log('uploaded', filePath)
        }
     
        console.log('Done...')
        await publishLog(`Done`)
        process.exit(0);
    })
}

init()