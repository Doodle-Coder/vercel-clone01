const express = require('express')
const { generateSlug } = require('random-word-slugs')
const { ECSClient, RunTaskCommand } = require('@aws-sdk/client-ecs')
const { Server } = require('socket.io');
const cors = require('cors');
const {z} = require('zod');
const {PrismaClient} = require('@prisma/client');
const { createClient } = require('@clickhouse/client')
const { Kafka } = require('kafkajs')
const {v4:uuidv4} = require('uuid')
const fs =require('fs')
const path = require('path')





const prisma = new PrismaClient({})

const io = new Server({ cors: '*' })

const kafka = new Kafka({
    clientId:`api-server`,
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

const client = createClient({
    host:'https://clickhouse-2be7c85a-jb673372-2b87.a.aivencloud.com:14074',
    database:'default',
    username:'avnadmin',
    password:'AVNS_qifZ_20jsAaqB_T3hC8'    
})


const consumer = kafka.consumer({groupId: 'api-server-logs-consumer'})


io.on('connection', socket => {
    socket.on('subscribe', channel => {
        socket.join(channel)
        socket.emit('message', `Joined ${channel}`)
    })
})

io.listen(9001,()=>{
    console.log("Socker Server running on PORT:9001 ")
})

const app = express()
const PORT = 9000

const ecsClient = new ECSClient({
    region:'ap-south-1',
    credentials: {
        accessKeyId:'AKIA6EB6EDAB6JPIQI5S',
        secretAccessKey:'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'
    }
})

const config = {
    CLUSTER: 'arn:aws:ecs:ap-south-1:970792900611:cluster/builder-cluster',
    TASK: 'arn:aws:ecs:ap-south-1:970792900611:task-definition/builder-task'
}

app.use(express.json())
app.use(cors())

app.post('/project',async (req,res)=>{
    const schema = z.object({
        name: z.string(),
        gitUrl: z.string()
    })
    const safeParseResult = schema.safeParse(req.body);
    if(safeParseResult.error) return res.status(400).json({error: safeParseResult.error})
    const {name , gitUrl} = safeParseResult.data

    const project = await prisma.project.create({
        data:{
            name,gitUrl,subDomain:generateSlug()
        }
    })
    return res.json({status:'success',data: { project } })
})

app.get('/logs/:id',async (req,res)=>{

    const id = req.params.id;
    const logs = await client.query({
        query:`SELECT event_id, deployment_id, log, timestamp from log_events where deployment_id = {deployment_id:String}`,
        query_params:{
            deployment_id: id
        },
        format: 'JSONEachRow'
    });
    const rawLogs = await logs.json();

    return res.json({logs:rawLogs});

})

app.post('/deploy', async (req, res) => {
    const { proejctId } = req.body
    
    const project = await prisma.project.findUnique({where :{ id: proejctId }})

    if(!project) return res.status(404).json({error: 'Project not found'})

    // Check If the deployment is already runnning or not 

    const deployment = await prisma.deployment.create({
        data:{
            project:{connect:{ id: proejctId }},
            status: 'QUEUED'
        }
    })

    // Spin the container
    console.log("Hello")
    const command = new RunTaskCommand({
        cluster: config.CLUSTER,
        taskDefinition: config.TASK,
        launchType: 'FARGATE',
        count: 1,
        networkConfiguration: {
            awsvpcConfiguration: {
                assignPublicIp: 'ENABLED',
                subnets: ["subnet-025b5c8fb75bd356a",
                "subnet-04dc96ea4f1acd393",
                "subnet-016cb0cef69a04814",],
                securityGroups: ['sg-097ae14996df21264']
            }
        },
        overrides: {
            containerOverrides: [
                {
                    name: 'builder-image',
                    environment: [
                        { name: 'GIT_REPOSITORY__URL', value: project.gitUrl },
                        { name: 'PROJECT_ID', value: proejctId },
                        { name: 'DEPLOYMENT_ID', value: deployment.id }
                    ]
                }
            ]
        }
    })
    await ecsClient.send(command);

    return res.json({ status: 'queued', data: {deploymentId:deployment.id} })

})


async function initkafkaConsumer(){
    await consumer.connect();
    await consumer.subscribe({topics:['container-logs']})

    await consumer.run({
        autoCommit:false,
        eachBatch: async function({ batch, heartbeat, commitOffsetsIfNecessary,resolveOffset}){
            const messages = batch.messages;
            console.log(`Rec. ${messages.length} messages..`)
            for(const message of messages){
                const stringMessage = message.value.toString();
                const {PROJECT_ID,DEPLOYMENT_ID,log} = JSON.parse(stringMessage);
                console.log({ log, DEPLOYMENT_ID },"Hello watch this")
                const { query_id } = await client.insert({
                    table: 'log_events',
                    values: [{ event_id: uuidv4(), deployment_id: DEPLOYMENT_ID, log}],
                    format: 'JSONEachRow'
                })
                console.log(query_id,"Created...");
                resolveOffset(message.offset);
                await commitOffsetsIfNecessary(message.offset);
                await heartbeat();
            }
        }
    })
}

initkafkaConsumer();
app.listen(PORT, () => console.log(`API Server Running...${PORT}`))

